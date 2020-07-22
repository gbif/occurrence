package org.gbif.occurrence.download.file.common;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
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
import java.time.Duration;
import java.util.function.Consumer;

/**
 * Executes a Search query and applies a predicate to each result.
 */
public class SearchQueryProcessor {

  // Default page size for queries.
  private static final int LIMIT = 300;

  private static final String KEY_FIELD = OccurrenceEsField.GBIF_ID.getFieldName();

  private static final Logger LOG = LoggerFactory.getLogger(SearchQueryProcessor.class);

  private static final RetryRegistry RETRY_REGISTRY = RetryRegistry.of(RetryConfig.custom()
                                                        .maxAttempts(5)
                                                        .intervalFunction(IntervalFunction.ofExponentialBackoff(Duration.ofSeconds(3), 2d))
                                                        .build());

  /**
   * Executes a query and applies the predicate to each result.
   *
   * @param downloadFileWork it's used to determine how to page through the results and the search query to be used
   * @param downloadFileWork it's used to determine how to page through the results and the search query to be used
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
        searchSourceBuilder.fetchSource(null, "all"); //All field is not needed in the response
        SearchRequest searchRequest = new SearchRequest().indices(downloadFileWork.getEsIndex()).source(searchSourceBuilder);

        SearchResponse searchResponse = trySearch(searchRequest, downloadFileWork.getEsClient());
        consume(searchResponse, resultHandler);

        SearchHit[] searchHits = searchResponse.getHits().getHits();
        recordCount += searchHits.length;

      }
    } catch (Exception ex) {
      LOG.error("Error querying Elasticsearch", ex);
      throw Throwables.propagate(ex);
    }
  }

  /**
   * This functions acts as a defensive retry mechanism to perform queries against an Elasticsearch cluster.
   * The index being queried can potentially being modified.
   */
  private static SearchResponse trySearch(SearchRequest searchRequest, RestHighLevelClient restHighLevelClient) {
    return Retry.decorateSupplier(RETRY_REGISTRY.retry("SearchRetry"), () -> {
            try {
              return restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            } catch (IOException ex) {
              throw new RuntimeException(ex);
            }
          }
          ).get();
  }

  private static void consume(SearchResponse searchResponse, Consumer<Occurrence> consumer) {
    EsResponseParser.buildDownloadResponse(searchResponse, new PagingRequest(0, searchResponse.getHits().getHits().length))
      .getResults().forEach(consumer);
  }
  /**
   * Creates a search query that contains the query parameter as the filter query value.
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
