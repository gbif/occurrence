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

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.event.Event;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.occurrence.search.SearchException;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import co.elastic.clients.elasticsearch.core.search.Hit;
import jakarta.annotation.Nullable;

/**
 * Reads the records stored in HBase into the API model. The records are stored as the JSON the API
 * returns, so they're read with the same Jackson configuration as the web services.
 *
 * <p>With a {@link RecordsFallback}, the records missing from the table, or all of them when the
 * table can't be read, are tried once more from the fallback (the Elasticsearch _source while the
 * indices keep it).
 *
 * @param <T> interpreted record type
 */
public class RecordsReader<T extends VerbatimOccurrence> {

  private static final Logger LOG = LoggerFactory.getLogger(RecordsReader.class);

  private static final ObjectMapper MAPPER = JacksonJsonObjectMapperProvider.getObjectMapper();

  private final RecordsStore store;
  private final Class<T> type;
  @Nullable private final RecordsFallback<T> fallback;

  public RecordsReader(RecordsStore store, Class<T> type) {
    this(store, type, null);
  }

  public RecordsReader(RecordsStore store, Class<T> type, @Nullable RecordsFallback<T> fallback) {
    this.store = store;
    this.type = type;
    this.fallback = fallback;
  }

  public static RecordsReader<Occurrence> occurrences(RecordsStore store) {
    return new RecordsReader<>(store, Occurrence.class);
  }

  public static RecordsReader<Event> events(RecordsStore store) {
    return new RecordsReader<>(store, Event.class);
  }

  /** Same reader, trying the records missing from the table in the fallback. */
  public RecordsReader<T> withFallback(@Nullable RecordsFallback<T> fallback) {
    return new RecordsReader<>(store, type, fallback);
  }

  @Nullable
  public T get(String id) {
    return get(List.of(id)).stream().findFirst().orElse(null);
  }

  @Nullable
  public VerbatimOccurrence getVerbatim(String id) {
    return read(
            List.of(id),
            false,
            true,
            (row, records) -> records.put(row.id(), read(row.verbatim(), VerbatimOccurrence.class)),
            RecordsFallback::getVerbatim)
        .stream()
        .findFirst()
        .orElse(null);
  }

  /** Records in the order of the ids, skipping the ones that don't exist. */
  public List<T> get(List<String> ids) {
    return read(
        ids,
        true,
        false,
        (row, records) -> records.put(row.id(), read(row.interpreted(), type)),
        RecordsFallback::get);
  }

  /**
   * Like {@link #get(List)}, but the verbatim fields of the records are the complete verbatim
   * record. The interpreted view of the API leaves out the verbatim terms that have an interpreted
   * value, downloads need all of them.
   */
  public List<T> getWithAllVerbatimFields(List<String> ids) {
    return read(
        ids,
        true,
        true,
        (row, records) -> {
          T record = read(row.interpreted(), type);
          if (row.verbatim() != null) {
            VerbatimOccurrence verbatim = read(row.verbatim(), VerbatimOccurrence.class);
            record.setVerbatimFields(verbatim.getVerbatimFields());
          }
          records.put(row.id(), record);
        },
        RecordsFallback::get);
  }

  /** Maps a page of Elasticsearch hits, which only need their ids, to records. */
  public Function<List<Hit<Map<String, Object>>>, List<T>> hitsMapper() {
    return hits -> get(ids(hits));
  }

  public static List<String> ids(List<? extends Hit<?>> hits) {
    return hits.stream().map(Hit::id).toList();
  }

  /**
   * Reads the rows of the ids from the table, then the missing ones from the fallback, and returns
   * the records in the order of the ids.
   */
  private <R> List<R> read(
      List<String> ids,
      boolean interpreted,
      boolean verbatim,
      BiConsumer<RecordsStore.Row, Map<String, R>> rowReader,
      FallbackReader<T, R> fallbackReader) {
    Map<String, R> records = new HashMap<>();
    for (RecordsStore.Row row : rows(ids, interpreted, verbatim)) {
      rowReader.accept(row, records);
    }

    if (fallback != null && records.size() < ids.size()) {
      List<String> missing = ids.stream().filter(id -> !records.containsKey(id)).distinct().toList();
      LOG.debug("Reading {} records missing from the records table from the fallback", missing.size());
      records.putAll(fallbackReader.read(fallback, missing));
    }

    List<R> ordered = new ArrayList<>(records.size());
    for (String id : ids) {
      R record = records.get(id);
      if (record != null) {
        ordered.add(record);
      }
    }
    return ordered;
  }

  /** Rows of the table, none when it can't be read and there is a fallback to read them from. */
  private List<RecordsStore.Row> rows(List<String> ids, boolean interpreted, boolean verbatim) {
    try {
      return store.get(ids, interpreted, verbatim);
    } catch (ServiceUnavailableException e) {
      if (fallback == null) {
        throw e;
      }
      LOG.warn("Could not read the records table, reading {} records from the fallback", ids.size(), e);
      return List.of();
    }
  }

  @FunctionalInterface
  private interface FallbackReader<T extends VerbatimOccurrence, R> {
    Map<String, ? extends R> read(RecordsFallback<T> fallback, List<String> ids);
  }

  private static <R> R read(String json, Class<R> recordType) {
    try {
      return MAPPER.readValue(json, recordType);
    } catch (IOException e) {
      throw new SearchException("Could not read record " + recordType.getSimpleName(), e);
    }
  }
}
