/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.cli.common;

import org.gbif.occurrence.search.SearchException;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.TermsQueryField;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.DeleteByQueryRequest;
import co.elastic.clients.elasticsearch.core.DeleteByQueryResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;

/** Utility class to make ES queries. */
public class EsHelper {

  private static final Logger LOG = LoggerFactory.getLogger(EsHelper.class);

  private static final String AGG_BY_INDEX = "index_aggs";
  private static final String DATASET_KEY_FIELD = "datasetKey";
  private static final String METADATA_DATASET_KEY_FIELD = "metadata.datasetKey";

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
      final ElasticsearchClient esClient, String datasetKey, String[] aliases) {
    Objects.requireNonNull(esClient);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(datasetKey), "datasetKey is required");
    Preconditions.checkArgument(aliases != null && aliases.length > 0, "aliases are required");
    List<FieldValue> datasetKeyValues = List.of(FieldValue.of(datasetKey));

    Query query = Query.of(q -> q.bool(BoolQuery.of(b -> b
        .should(s -> s.terms(t -> t.field(DATASET_KEY_FIELD).terms(TermsQueryField.of(tf -> tf.value(datasetKeyValues)))))
        .should(s -> s.terms(t -> t.field(METADATA_DATASET_KEY_FIELD).terms(TermsQueryField.of(tf -> tf.value(datasetKeyValues)))))
    )));

    SearchRequest request = SearchRequest.of(s -> s
        .index(java.util.Arrays.asList(aliases))
        .size(0)
        .query(query)
        .aggregations(AGG_BY_INDEX, a -> a.terms(t -> t.field("_index")))
    );

    try {
      SearchResponse<Void> response = esClient.search(request, Void.class);
      return parseFindExistingIndexesInAliasResponse(response);
    } catch (IOException e) {
      throw new SearchException("Could not find indexes that contain the dataset " + datasetKey, e);
    }
  }

  private static Set<String> parseFindExistingIndexesInAliasResponse(SearchResponse<?> response) {
    Aggregate agg = response.aggregations().get(AGG_BY_INDEX);
    if (agg == null || !agg.isSterms()) {
      return java.util.Collections.emptySet();
    }
    return agg.sterms().buckets().array().stream()
        .map(b -> b.key().stringValue())
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());
  }

  /**
   * Deletes all the documents of a given dataset in a given index.
   *
   * @param esClient client to connect to ES
   * @param datasetKey key of the dataset whose documents will be deleted
   * @param index index where the documents will be deleted from
   */
  public static void deleteByDatasetKey(
      final ElasticsearchClient esClient, String datasetKey, String index) {
    LOG.info("Deleting all documents of dataset {} from ES index {}", datasetKey, index);
    Objects.requireNonNull(esClient);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(datasetKey), "datasetKey is required");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(index), "index is required");
    List<FieldValue> datasetKeyValues = List.of(FieldValue.of(datasetKey));

    Query query = Query.of(q -> q.bool(BoolQuery.of(b -> b
        .should(s -> s.terms(t -> t.field(DATASET_KEY_FIELD).terms(TermsQueryField.of(tf -> tf.value(datasetKeyValues)))))
        .should(s -> s.terms(t -> t.field(METADATA_DATASET_KEY_FIELD).terms(TermsQueryField.of(tf -> tf.value(datasetKeyValues)))))
    )));

    DeleteByQueryRequest request = DeleteByQueryRequest.of(d -> d
        .index(index)
        .query(query)
        .scrollSize(5000L)
    );

    try {
      DeleteByQueryResponse response = esClient.deleteByQuery(request);
      LOG.info("Deleted {} documents from the index {}", response.deleted(), index);
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
  public static void deleteIndex(final ElasticsearchClient esClient, String index) {
    LOG.info("Deleting ES index {}", index);
    Objects.requireNonNull(esClient);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(index), "index is required");

    DeleteIndexRequest request = DeleteIndexRequest.of(r -> r.index(index));
    try {
      esClient.indices().delete(request);
    } catch (IOException e) {
      LOG.error("Could not delete index {}", index);
    }
  }
}
