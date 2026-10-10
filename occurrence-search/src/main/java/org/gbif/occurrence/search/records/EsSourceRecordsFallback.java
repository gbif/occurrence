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
package org.gbif.occurrence.search.records;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.occurrence.search.SearchException;
import org.gbif.occurrence.search.es.BaseEsSearchRequestBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.search.Hit;

/**
 * Builds the records from the _source of their Elasticsearch documents, fetched by id. Documents of
 * indices with the _source disabled come without it and are skipped, as if they didn't exist.
 *
 * @param <T> interpreted record type
 */
public class EsSourceRecordsFallback<T extends VerbatimOccurrence> implements RecordsFallback<T> {

  private final ElasticsearchClient esClient;
  private final String esIndex;
  private final Function<Hit<Map<String, Object>>, T> toRecord;
  private final Function<Hit<Map<String, Object>>, VerbatimOccurrence> toVerbatim;

  public EsSourceRecordsFallback(
      ElasticsearchClient esClient,
      String esIndex,
      Function<Hit<Map<String, Object>>, T> toRecord,
      Function<Hit<Map<String, Object>>, VerbatimOccurrence> toVerbatim) {
    this.esClient = esClient;
    this.esIndex = esIndex;
    this.toRecord = toRecord;
    this.toVerbatim = toVerbatim;
  }

  @Override
  public Map<String, T> get(List<String> ids) {
    return read(ids, toRecord);
  }

  @Override
  public Map<String, VerbatimOccurrence> getVerbatim(List<String> ids) {
    return read(ids, toVerbatim);
  }

  private <R> Map<String, R> read(List<String> ids, Function<Hit<Map<String, Object>>, R> mapper) {
    Map<String, R> records = new HashMap<>();
    if (ids.isEmpty()) {
      return records;
    }
    SearchRequest request =
        SearchRequest.of(
            s ->
                s.index(esIndex)
                    .size(ids.size())
                    .query(q -> q.ids(i -> i.values(ids)))
                    .source(BaseEsSearchRequestBuilder.sourceConfig(true)));
    try {
      List<Hit<Map<String, Object>>> hits =
          esClient
              .search(request, (Class<Map<String, Object>>) (Class<?>) Map.class)
              .hits()
              .hits();
      for (Hit<Map<String, Object>> hit : hits) {
        if (hit.source() != null) {
          records.put(hit.id(), mapper.apply(hit));
        }
      }
      return records;
    } catch (IOException e) {
      throw new SearchException(e);
    }
  }
}
