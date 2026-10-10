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
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import co.elastic.clients.elasticsearch.core.search.Hit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RecordsReaderTest {

  /** Same configuration as the mapper pipelines writes the records with. */
  private static final ObjectMapper API_MAPPER = JacksonJsonObjectMapperProvider.getObjectMapper();

  private static final UUID DATASET_KEY = UUID.fromString("38b4c89f-584c-41bb-bd8f-cd1def33e92f");

  /** In-memory records table. */
  private static RecordsStore store(Map<String, RecordsStore.Row> rows) {
    return (ids, interpreted, verbatim) ->
        ids.stream()
            .filter(rows::containsKey)
            .map(rows::get)
            .map(
                r ->
                    new RecordsStore.Row(
                        r.id(), interpreted ? r.interpreted() : null, verbatim ? r.verbatim() : null))
            .collect(Collectors.toList());
  }

  private static Occurrence occurrence(long key) {
    Occurrence occ = new Occurrence();
    occ.setKey(key);
    occ.setDatasetKey(DATASET_KEY);
    occ.setCountry(Country.DENMARK);
    occ.setDecimalLatitude(55.68);
    occ.setDecimalLongitude(12.57);
    occ.setScientificName("Puma concolor (Linnaeus, 1771)");
    occ.setIssues(Set.of(OccurrenceIssue.COORDINATE_ROUNDED));
    // the interpreted view leaves out the verbatim terms that have an interpreted value
    occ.getVerbatimFields().put(DwcTerm.occurrenceID, "occ-" + key);
    Map<Term, String> extensionRecord = new HashMap<>();
    extensionRecord.put(DwcTerm.measurementType, "weight");
    occ.setExtensions(Map.of("http://rs.tdwg.org/dwc/terms/MeasurementOrFact", List.of(extensionRecord)));
    return occ;
  }

  private static VerbatimOccurrence verbatim(long key) {
    VerbatimOccurrence v = new VerbatimOccurrence();
    v.setKey(key);
    v.setDatasetKey(DATASET_KEY);
    v.getVerbatimFields().put(GbifTerm.gbifID, String.valueOf(key));
    v.getVerbatimFields().put(DwcTerm.occurrenceID, "occ-" + key);
    v.getVerbatimFields().put(DwcTerm.scientificName, "Puma concolor");
    v.getVerbatimFields().put(DwcTerm.country, "Denmark");
    return v;
  }

  private static RecordsStore.Row row(long key) throws JsonProcessingException {
    return new RecordsStore.Row(
        String.valueOf(key),
        API_MAPPER.writeValueAsString(occurrence(key)),
        API_MAPPER.writeValueAsString(verbatim(key)));
  }

  private static RecordsReader<Occurrence> reader(long... keys) throws JsonProcessingException {
    Map<String, RecordsStore.Row> rows = new HashMap<>();
    for (long key : keys) {
      rows.put(String.valueOf(key), row(key));
    }
    return RecordsReader.occurrences(store(rows));
  }

  @Test
  void interpretedRecordsAreReadAsTheApiReturnsThem() throws Exception {
    Occurrence occ = reader(1L).get("1");

    // read records are serialized by the API exactly as they were stored
    assertEquals(
        API_MAPPER.readTree(API_MAPPER.writeValueAsString(occurrence(1L))),
        API_MAPPER.readTree(API_MAPPER.writeValueAsString(occ)));
    assertEquals(1L, occ.getKey());
    assertEquals(Country.DENMARK, occ.getCountry());
    assertEquals(Set.of(OccurrenceIssue.COORDINATE_ROUNDED), occ.getIssues());
    assertEquals("occ-1", occ.getVerbatimField(DwcTerm.occurrenceID));
    assertEquals(
        "weight",
        occ.getExtensions()
            .get("http://rs.tdwg.org/dwc/terms/MeasurementOrFact")
            .get(0)
            .get(DwcTerm.measurementType));
  }

  @Test
  void verbatimRecordsAreRead() throws Exception {
    VerbatimOccurrence v = reader(1L).getVerbatim("1");

    assertEquals(verbatim(1L), v);
  }

  @Test
  void missingRecordsAreSkippedKeepingTheOrder() throws Exception {
    RecordsReader<Occurrence> reader = reader(1L, 2L, 3L);

    List<Occurrence> records = reader.get(List.of("3", "4", "1"));

    assertEquals(List.of(3L, 1L), records.stream().map(Occurrence::getKey).toList());
    assertNull(reader.get("4"));
    assertNull(reader.getVerbatim("4"));
  }

  @Test
  void hitsAreMappedByTheirIds() throws Exception {
    List<Hit<Map<String, Object>>> hits =
        List.of(
            Hit.of(h -> h.index("occurrence").id("2")), Hit.of(h -> h.index("occurrence").id("1")));

    List<Occurrence> records = reader(1L, 2L).hitsMapper().apply(hits);

    assertEquals(List.of(2L, 1L), records.stream().map(Occurrence::getKey).toList());
  }

  @Test
  void downloadRecordsHaveAllVerbatimFields() throws Exception {
    Occurrence occ = reader(1L).getWithAllVerbatimFields(List.of("1")).get(0);

    assertEquals(verbatim(1L).getVerbatimFields(), occ.getVerbatimFields());
    assertEquals("Puma concolor (Linnaeus, 1771)", occ.getScientificName());
    assertFalse(occ.getExtensions().isEmpty());
  }

  /** Fallback with the records of the keys, recording the ids it's asked for. */
  private static RecordsFallback<Occurrence> fallback(List<List<String>> requests, long... keys) {
    Map<String, Occurrence> records = new HashMap<>();
    for (long key : keys) {
      Occurrence occ = occurrence(key);
      occ.setScientificName("from the fallback");
      records.put(String.valueOf(key), occ);
    }
    return new RecordsFallback<>() {
      @Override
      public Map<String, Occurrence> get(List<String> ids) {
        requests.add(ids);
        return ids.stream()
            .filter(records::containsKey)
            .collect(Collectors.toMap(id -> id, records::get));
      }

      @Override
      public Map<String, VerbatimOccurrence> getVerbatim(List<String> ids) {
        requests.add(ids);
        return ids.stream()
            .filter(records::containsKey)
            .collect(Collectors.toMap(id -> id, id -> verbatim(Long.parseLong(id))));
      }
    };
  }

  @Test
  void missingRecordsAreReadFromTheFallbackKeepingTheOrder() throws Exception {
    List<List<String>> requests = new ArrayList<>();
    RecordsReader<Occurrence> reader = reader(1L, 3L).withFallback(fallback(requests, 2L, 3L));

    List<Occurrence> records = reader.get(List.of("3", "2", "4", "1"));

    assertEquals(List.of(3L, 2L, 1L), records.stream().map(Occurrence::getKey).toList());
    // records in the table aren't read from the fallback
    assertEquals("Puma concolor (Linnaeus, 1771)", records.get(0).getScientificName());
    assertEquals("from the fallback", records.get(1).getScientificName());
    assertEquals(List.of(List.of("2", "4")), requests);
  }

  @Test
  void fallbackIsNotQueriedWhenAllRecordsAreInTheTable() throws Exception {
    List<List<String>> requests = new ArrayList<>();
    RecordsReader<Occurrence> reader = reader(1L, 2L).withFallback(fallback(requests, 1L, 2L));

    reader.get(List.of("1", "2"));
    reader.getWithAllVerbatimFields(List.of("1", "2"));

    assertEquals(List.of(), requests);
  }

  @Test
  void singleRecordsAreReadFromTheFallback() throws Exception {
    List<List<String>> requests = new ArrayList<>();
    RecordsReader<Occurrence> reader = reader().withFallback(fallback(requests, 5L));

    assertEquals("from the fallback", reader.get("5").getScientificName());
    assertEquals(verbatim(5L), reader.getVerbatim("5"));
    assertNull(reader.get("6"));
    assertNull(reader.getVerbatim("6"));
  }

  @Test
  void allRecordsAreReadFromTheFallbackWhenTheTableFails() {
    RecordsStore failing =
        (ids, interpreted, verbatim) -> {
          throw new ServiceUnavailableException("HBase down");
        };
    List<List<String>> requests = new ArrayList<>();

    List<Occurrence> records =
        RecordsReader.occurrences(failing).withFallback(fallback(requests, 1L, 2L)).get(List.of("2", "1"));

    assertEquals(List.of(2L, 1L), records.stream().map(Occurrence::getKey).toList());
    assertThrows(ServiceUnavailableException.class, () -> RecordsReader.occurrences(failing).get(List.of("1")));
  }

  @Test
  void rowKeysMatchPipelines() {
    assertEquals("67:1234567", RecordsTable.occurrenceRowKey(1234567L));
    assertEquals("02:4000002", RecordsTable.occurrenceRowKey("4000002"));
    assertEquals("00:100", RecordsTable.occurrenceRowKey(100L));
    assertEquals("05:5", RecordsTable.occurrenceRowKey(5L));
    assertEquals(
        "cbf64c0df611eae2fc0c2a3234f0eeac8f423071",
        RecordsTable.eventRowKey("cbf64c0df611eae2fc0c2a3234f0eeac8f423071"));
  }
}
