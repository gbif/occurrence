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
package org.gbif.occurrence.download.elastic;

import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.occurrence.download.predicate.EsPredicateUtil;
import org.gbif.occurrence.download.query.QueryVisitorsFactory;
import org.gbif.occurrence.search.es.OccurrenceBaseEsFieldMapper;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Builder
@Slf4j
public class DownloadEsClient implements Closeable {

  private static final ObjectMapper OBJECT_MAPPER =
    new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  static {
    OBJECT_MAPPER.addMixIn(SearchParameter.class, QueryVisitorsFactory.OccurrenceSearchParameterMixin.class);
  }

  private final RestHighLevelClient esClient;

  private final String esIndex;


  private final OccurrenceBaseEsFieldMapper esFieldMapper;

  @SneakyThrows
  private Predicate toPredicateApi(org.gbif.api.model.occurrence.predicate.Predicate predicate) {
    return OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(predicate), Predicate.class);
  }

  /**
   * Executes the ElasticSearch query and returns the number of records found.
   * If an error occurs 'ERROR_COUNT' is returned.
   */
  @SneakyThrows
  public long getRecordCount(org.gbif.api.model.occurrence.predicate.Predicate requestPredicate) {

    Predicate predicate = toPredicateApi(requestPredicate);
    CountResponse response = esClient.count(new CountRequest().indices(esIndex).query(EsPredicateUtil.searchQuery(predicate, esFieldMapper)),
      RequestOptions.DEFAULT);
    log.info("Download record count {}", response.getCount());
    return response.getCount();
  }

  /**
   * Shuts down the ElasticSearch client.
   */
  private void shutDownEsClientSilently() {
    try {
      if (Objects.nonNull(esClient)) {
        esClient.close();
      }
    } catch (IOException ex) {
      log.error("Error shutting down Elasticsearch client", ex);
    }
  }

  @Override
  public void close() {
    shutDownEsClientSilently();
  }
}
