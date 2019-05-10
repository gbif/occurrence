package org.gbif.occurrence.download.file.common;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.occurrence.download.file.DownloadFileWork;
import org.gbif.occurrence.search.es.EsResponseParser;
import org.gbif.occurrence.search.es.OccurrenceEsField;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Executes a Search query and applies a predicate to each result.
 */
public class SearchQueryProcessor {

  // Default page size for Solr queries.
  private static final int LIMIT = 300;

  private static final String KEY_FIELD = OccurrenceEsField.GBIF_ID.getFieldName();

  /**
   * Executes a query on the SolrServer parameter and applies the predicate to each result.
   *
   * @param downloadFileWork it's used to determine how to page through the results and the Solr query to be used
   * @param resultHandler    predicate that process each result, receives as parameter the occurrence key
   */
  public static void processQuery(DownloadFileWork downloadFileWork, Consumer<Occurrence> resultHandler) {

    // Calculates the amount of output records
    int nrOfOutputRecords = downloadFileWork.getTo() - downloadFileWork.getFrom();

    // Creates a search request instance using the search request that comes in the fileJob
    SearchSourceBuilder searchSourceBuilder = createSearchQuery(downloadFileWork.getQuery());
    //key is required since this runs in a distributed installations where the natural order can't be guaranteed
    searchSourceBuilder.sort(KEY_FIELD, SortOrder.DESC);

    try {
      int recordCount = 0;
      while (recordCount < nrOfOutputRecords) {
        int from = downloadFileWork.getFrom() + recordCount;
        int limit = recordCount + LIMIT > nrOfOutputRecords ? nrOfOutputRecords - recordCount : LIMIT;
        searchSourceBuilder.from(from);
        // Limit can't be greater than the maximum number of records assigned to this job
        searchSourceBuilder.size(limit);
        SearchResponse response = downloadFileWork.getEsClient().search(new SearchRequest().indices(downloadFileWork.getEsIndex())
                                                                          .source(searchSourceBuilder), RequestOptions.DEFAULT);

        EsResponseParser.buildResponse(response, new PagingRequest(from, limit)).getResults().forEach(resultHandler);

        recordCount += response.getHits().getHits().length;
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Creates a SolrQuery that contains the query parameter as the filter query value.
   */
  private static SearchSourceBuilder createSearchQuery(String query) {
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    if (!Strings.isNullOrEmpty(query)) {
      searchSourceBuilder.query(QueryBuilders.wrapperQuery(query));
    }
    return searchSourceBuilder;
  }

  /**
   * Hidden constructor.
   */
  private SearchQueryProcessor() {
    //empty constructor
  }
}
