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

import org.gbif.api.model.event.Event;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.occurrence.search.SearchException;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;

import co.elastic.clients.elasticsearch.core.search.Hit;
import jakarta.annotation.Nullable;

/**
 * Reads the records stored in HBase into the API model. The records are stored as the JSON the API
 * returns, so they're read with the same Jackson configuration as the web services.
 *
 * @param <T> interpreted record type
 */
public class RecordsReader<T extends VerbatimOccurrence> {

  private static final ObjectMapper MAPPER = JacksonJsonObjectMapperProvider.getObjectMapper();

  private final RecordsStore store;
  private final Class<T> type;

  public RecordsReader(RecordsStore store, Class<T> type) {
    this.store = store;
    this.type = type;
  }

  public static RecordsReader<Occurrence> occurrences(RecordsStore store) {
    return new RecordsReader<>(store, Occurrence.class);
  }

  public static RecordsReader<Event> events(RecordsStore store) {
    return new RecordsReader<>(store, Event.class);
  }

  @Nullable
  public T get(String id) {
    return store.get(List.of(id), true, false).stream()
        .findFirst()
        .map(row -> read(row.interpreted(), type))
        .orElse(null);
  }

  @Nullable
  public VerbatimOccurrence getVerbatim(String id) {
    return store.get(List.of(id), false, true).stream()
        .findFirst()
        .map(row -> read(row.verbatim(), VerbatimOccurrence.class))
        .orElse(null);
  }

  /** Records in the order of the ids, skipping the ones that don't exist. */
  public List<T> get(List<String> ids) {
    return store.get(ids, true, false).stream().map(row -> read(row.interpreted(), type)).toList();
  }

  /**
   * Like {@link #get(List)}, but the verbatim fields of the records are the complete verbatim
   * record. The interpreted view of the API leaves out the verbatim terms that have an interpreted
   * value, downloads need all of them.
   */
  public List<T> getWithAllVerbatimFields(List<String> ids) {
    return store.get(ids, true, true).stream()
        .map(
            row -> {
              T record = read(row.interpreted(), type);
              if (row.verbatim() != null) {
                VerbatimOccurrence verbatim = read(row.verbatim(), VerbatimOccurrence.class);
                record.setVerbatimFields(verbatim.getVerbatimFields());
              }
              return record;
            })
        .toList();
  }

  /** Maps a page of Elasticsearch hits, which only need their ids, to records. */
  public Function<List<Hit<Map<String, Object>>>, List<T>> hitsMapper() {
    return hits -> get(ids(hits));
  }

  public static List<String> ids(List<? extends Hit<?>> hits) {
    return hits.stream().map(Hit::id).toList();
  }

  private static <R> R read(String json, Class<R> recordType) {
    try {
      return MAPPER.readValue(json, recordType);
    } catch (IOException e) {
      throw new SearchException("Could not read record " + recordType.getSimpleName(), e);
    }
  }
}
