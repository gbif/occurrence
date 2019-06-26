package org.gbif.occurrence.cli.common;

import org.gbif.occurrence.search.SearchException;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.Strings;

import static org.gbif.occurrence.search.es.EsQueryUtils.HEADERS;

import static org.elasticsearch.client.RequestOptions.DEFAULT;

/** Utility class to make ES queries. */
public class EsHelper {

  private static final Logger LOG = LoggerFactory.getLogger(EsHelper.class);

  private static final String AGG_BY_INDEX = "index_aggs";
  private static final String DATASET_KEY_FIELD = "datasetKey";

  private EsHelper() {}

  /**
   * Finds all the indexes of the alias where the given datasetKey is indexed.
   *
   * @param esClient client to connect to ES
   * @param datasetKey datasetKey to look for
   * @param aliases index or alias where we are looking for indexes
   * @return indexes found
   */
  public static Set<String> findExistingIndexesInAliases(
      final RestHighLevelClient esClient, String datasetKey, String[] aliases) {
    Objects.requireNonNull(esClient);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(datasetKey), "datasetKey is required");
    Preconditions.checkArgument(aliases != null && aliases.length > 0, "aliases are required");

    SearchRequest esRequest = new SearchRequest();
    esRequest.indices(aliases);

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(0);
    esRequest.source(searchSourceBuilder);

    // add match query to filter by datasetKey
    searchSourceBuilder.query(QueryBuilders.matchQuery(DATASET_KEY_FIELD, datasetKey));
    // add aggs by index
    searchSourceBuilder.aggregation(AggregationBuilders.terms(AGG_BY_INDEX).field("_index"));

    try {
      return parseFindExistingIndexesInAliasResponse(esClient.search(esRequest, HEADERS.get()));
    } catch (IOException e) {
      throw new SearchException("Could not find indexes that contain the dataset " + datasetKey, e);
    }
  }

  private static Set<String> parseFindExistingIndexesInAliasResponse(SearchResponse response) {
    return ((Terms) response.getAggregations().get(AGG_BY_INDEX))
        .getBuckets().stream()
            .map(MultiBucketsAggregation.Bucket::getKeyAsString)
            .collect(Collectors.toSet());
  }

  /**
   * Deletes all the documents of a given dataset in a given index.
   *
   * @param esClient client to connect to ES
   * @param datasetKey key of the dataset whose documents will be deleted
   * @param index index where the the documents will be deleted from
   */
  public static void deleteByDatasetKey(
      final RestHighLevelClient esClient, String datasetKey, String index) {
    Objects.requireNonNull(esClient);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(datasetKey), "datasetKey is required");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(index), "index is required");

    DeleteByQueryRequest request =
        new DeleteByQueryRequest(index)
            .setBatchSize(5000)
            .setQuery(QueryBuilders.matchQuery(DATASET_KEY_FIELD, datasetKey));

    try {
      esClient.deleteByQuery(request, HEADERS.get());
    } catch (IOException e) {
      LOG.error("Could not delete records of dataset {} from index {}", datasetKey, index);
    }
  }

  /**
   * Deletes an ES index.
   *
   * @param esClient client to connect to ES
   * @param index index to delete
   */
  public static void deleteIndex(final RestHighLevelClient esClient, String index) {
    Objects.requireNonNull(esClient);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(index), "index is required");

    DeleteIndexRequest request = new DeleteIndexRequest(index);
    try {
      esClient.indices().delete(request, DEFAULT);
    } catch (IOException e) {
      LOG.error("Could not delete index {}", index);
    }
  }
}
