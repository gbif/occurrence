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
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.occurrence.search.es.EsPredicateUtil;
import org.gbif.search.es.occurrence.OccurrenceEsFieldMapper;

import java.io.Closeable;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.CountResponse;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Builder
@Slf4j
public class DownloadEsClient implements Closeable {

  private static final ObjectMapper OBJECT_MAPPER =
    new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  static {
    // only used by ES downloads so forcing occurence since events don't fo thru ES downloads
    OBJECT_MAPPER.registerModule(
      new SimpleModule()
        .addKeyDeserializer(
          SearchParameter.class,
          new OccurrenceSearchParameter.OccurrenceSearchParameterKeyDeserializer())
        .addDeserializer(
          SearchParameter.class,
          new OccurrenceSearchParameter.OccurrenceSearchParameterDeserializer()));
  }

  private final ElasticsearchClient esClient;

  private final String esIndex;

  private final OccurrenceEsFieldMapper esFieldMapper;

  private final String defaultChecklistKey;

  /**
   * Executes the ElasticSearch query and returns the number of records found.
   * Throws SearchException on failure so callers can distinguish a real zero count.
   */
  public long getRecordCount(Predicate predicate) {
    try {
      CountResponse response =
          esClient.count(
              c ->
                  c.index(esIndex)
                      .query(EsPredicateUtil.searchQuery(predicate, esFieldMapper, defaultChecklistKey)));
      log.info("Download record count {}", response.count());
      return response.count();
    } catch (Exception ex) {
      log.error("Error counting download records", ex);
      throw new org.gbif.occurrence.search.SearchException(ex);
    }
  }

  @Override
  public void close() {
    try {
      esClient._transport().close();
    } catch (Exception ex) {
      log.error("Error closing Elasticsearch transport", ex);
    }
  }
}
