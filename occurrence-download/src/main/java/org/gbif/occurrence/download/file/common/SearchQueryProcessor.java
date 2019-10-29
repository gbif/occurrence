package org.gbif.occurrence.download.file.common;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.occurrence.download.file.DownloadFileWork;
import org.gbif.occurrence.search.es.EsResponseParser;
import org.gbif.occurrence.search.es.OccurrenceEsField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Executes a Search query and applies a predicate to each result.
 */
public class SearchQueryProcessor {

  // Default page size for queries.
  private static final int LIMIT = 300;

  //Keep alive time for the scroll session
  private static final long SCROLL_TIME_VALUE = 5L;

  private static final String KEY_FIELD = OccurrenceEsField.GBIF_ID.getFieldName();

  private static final Logger LOG = LoggerFactory.getLogger(SearchQueryProcessor.class);

  /**
   * Executes a query on the SolrServer parameter and applies the predicate to each result.
   *
   * @param downloadFileWork it's used to determine how to page through the results and the Solr query to be used
   * @param resultHandler    predicate that process each result, receives as parameter the occurrence key
   */
  public static void processQuery(DownloadFileWork downloadFileWork, Consumer<Occurrence> resultHandler) {

    // Calculates the amount of output records
    int nrOfOutputRecords = downloadFileWork.getTo() - downloadFileWork.getFrom();

    try {

      int recordCount = 0;
      // Creates a search request instance using the search request that comes in the fileJob
      SearchSourceBuilder searchSourceBuilder = createSearchQuery(downloadFileWork.getQuery());

      while (recordCount < nrOfOutputRecords) {

        searchSourceBuilder.size(recordCount + LIMIT > nrOfOutputRecords ? nrOfOutputRecords - recordCount : LIMIT);
        searchSourceBuilder.from(downloadFileWork.getFrom() + recordCount);
        SearchRequest searchRequest = new SearchRequest().indices(downloadFileWork.getEsIndex()).source(searchSourceBuilder);

        SearchResponse searchResponse = downloadFileWork.getEsClient().search(searchRequest, RequestOptions.DEFAULT);
        consume(searchResponse, resultHandler);

        SearchHit[] searchHits = searchResponse.getHits().getHits();
        recordCount += searchHits.length;

      }
    } catch (IOException ex) {
      throw Throwables.propagate(ex);
    }
  }

  private static void consume(SearchResponse searchResponse, Consumer<Occurrence> consumer) {
    EsResponseParser.buildDownloadResponse(searchResponse, new PagingRequest(0, searchResponse.getHits().getHits().length))
      .getResults().forEach(consumer);
  }
  /**
   * Creates a SolrQuery that contains the query parameter as the filter query value.
   */
  private static SearchSourceBuilder createSearchQuery(String query) {
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    if (!Strings.isNullOrEmpty(query)) {
      searchSourceBuilder.query(QueryBuilders.wrapperQuery(query));
    }
    //key is required since this runs in a distributed installations where the natural order can't be guaranteed
    searchSourceBuilder.sort(KEY_FIELD, SortOrder.DESC);
    return searchSourceBuilder;
  }

  /**
   * Hidden constructor.
   */
  private SearchQueryProcessor() {
    //empty constructor
  }

}
