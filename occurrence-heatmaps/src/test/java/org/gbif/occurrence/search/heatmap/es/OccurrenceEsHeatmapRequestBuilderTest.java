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
package org.gbif.occurrence.search.heatmap.es;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.search.es.ChecklistEsField;
import org.gbif.search.es.occurrence.OccurrenceEsField;
import org.gbif.search.heatmap.es.occurrence.OccurrenceEsHeatmapRequestBuilder;
import org.gbif.search.heatmap.occurrence.OccurrenceHeatmapRequest;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import co.elastic.clients.elasticsearch.core.SearchRequest;

import static org.gbif.occurrence.search.es.EsQueryUtils.*;
import static org.gbif.search.heatmap.es.BaseEsHeatmapRequestBuilder.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests for ElasticSearch heatmap request builders. */
public class OccurrenceEsHeatmapRequestBuilderTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String INDEX = "index";
  private final OccurrenceEsHeatmapRequestBuilder esHeatmapRequestBuilder =
      new OccurrenceEsHeatmapRequestBuilder(
          OccurrenceEsField.buildFieldMapper(), null, null, UUID.fromString("7ddf754f-d193-4cc9-b351-99906754a03b"), 100);

  @Test
  public void heatmapRequestTest() throws IOException {
    OccurrenceHeatmapRequest request = new OccurrenceHeatmapRequest();
    request.setGeometry("-44, 30, -32, 54");
    request.setZoom(1);

    SearchRequest query = esHeatmapRequestBuilder.buildHeatmapRequest(request, INDEX);
    JsonNode json = MAPPER.readTree(toJson(query));

    assertEquals(0, json.get(SIZE).asInt());

    // aggs
    assertTrue(json.path(QUERY).path(BOOL).path(FILTER).get(0).has(GEO_BOUNDING_BOX));

    // assert bbox
    JsonNode bbox =
        json.path(QUERY)
            .path(BOOL)
            .path(FILTER)
            .path(0)
            .path(GEO_BOUNDING_BOX)
            .path(OccurrenceEsField.COORDINATE_POINT.getSearchFieldName());
    assertEquals(54d, bbox.path("top").asDouble(), 0);
    assertEquals(-44d, bbox.path("left").asDouble(), 0);
    assertEquals(30d, bbox.path("bottom").asDouble(), 0);
    assertEquals(-32d, bbox.path("right").asDouble(), 0);

    // geohash_grid
    assertTrue(json.path(AGGREGATIONS).path(HEATMAP_AGGS).has(GEOHASH_GRID));
    JsonNode jsonGeohashGrid = json.path(AGGREGATIONS).path(HEATMAP_AGGS).path(GEOHASH_GRID);
    assertEquals(
        OccurrenceEsField.COORDINATE_POINT.getSearchFieldName(),
        jsonGeohashGrid.get(FIELD).asText());
    assertEquals(3, jsonGeohashGrid.get(PRECISION).asInt());

    // geo_bounds
    assertTrue(
        json.path(AGGREGATIONS)
            .path(HEATMAP_AGGS)
            .path(AGGREGATIONS)
            .path(CELL_AGGS)
            .has(GEO_BOUNDS));
    JsonNode jsonGeobounds =
        json.path(AGGREGATIONS)
            .path(HEATMAP_AGGS)
            .path(AGGREGATIONS)
            .path(CELL_AGGS)
            .path(GEO_BOUNDS);
    assertEquals(
        OccurrenceEsField.COORDINATE_POINT.getSearchFieldName(), jsonGeobounds.get(FIELD).asText());
  }

  /** Tries to find a field in the list of term filters. */
  private static Optional<String> findTermFilter(
      JsonNode node, ChecklistEsField field, String defaultChecklistKey) {
    String fieldName = field.getSearchFieldName(defaultChecklistKey);
    ArrayNode arrayNode =
        (ArrayNode) node.path(QUERY).path(BOOL).path(FILTER).get(2).path(BOOL).path(FILTER);
    return StreamSupport.stream(
            Spliterators.spliterator(arrayNode.elements(), 2, Spliterator.ORDERED), false)
        .filter(termNode -> termNode.path(TERM).has(fieldName))
        .map(termNode -> termNode.path(TERM).get(fieldName).get(VALUE).asText())
        .findFirst();
  }

  @Test
  public void heatmapRequestFilteredTest() throws IOException {
    OccurrenceHeatmapRequest request = new OccurrenceHeatmapRequest();
    request.addTaxonKeyFilter("4");
    request.setGeometry("-44, 30, -32, 54");
    request.setZoom(1);

    SearchRequest query = esHeatmapRequestBuilder.buildHeatmapRequest(request, INDEX);
    JsonNode json = MAPPER.readTree(toJson(query));

    assertEquals(0, json.get(SIZE).asInt());
    assertTrue(json.path(QUERY).path(BOOL).path(FILTER).isArray());
    assertTrue(json.path(QUERY).path(BOOL).path(FILTER).get(1).has(TERM));

    // taxon key
    Optional<String> taxaValue =
        findTermFilter(
            json,
            (ChecklistEsField) OccurrenceEsField.TAXON_KEY.getEsField(),
            "7ddf754f-d193-4cc9-b351-99906754a03b");

    if (taxaValue.isPresent()) {
      assertEquals(4, Integer.parseInt(taxaValue.get()));
    } else {
      fail("TaxaKey term not found");
    }

    // geohash_grid
    assertTrue(json.path(AGGREGATIONS).path(HEATMAP_AGGS).has(GEOHASH_GRID));

    // geo_bounds
    assertTrue(
        json.path(AGGREGATIONS)
            .path(HEATMAP_AGGS)
            .path(AGGREGATIONS)
            .path(CELL_AGGS)
            .has(GEO_BOUNDS));
  }

  private static final String DEFAULT_CHECKLIST_KEY = "7ddf754f-d193-4cc9-b351-99906754a03b";
  private static final UUID DNA_CHECKLIST_KEY =
      UUID.fromString("2d59e5db-57ad-41ff-97d6-11f5fb264527");

  private static final String TAXON_KEY_FIELD =
      ((ChecklistEsField) OccurrenceEsField.TAXON_KEY.getEsField())
          .getSearchFieldName(DEFAULT_CHECKLIST_KEY);

  private static String dnaTaxonKeyField(UUID checklistKey) {
    return OccurrenceEsField.buildFieldMapper()
        .getDnaChecklistField(checklistKey, OccurrenceSearchParameter.TAXON_KEY);
  }

  /** The fields of the term and terms queries in the heatmap query. */
  private static List<String> termFields(JsonNode json) {
    return Stream.concat(json.findValues(TERM).stream(), json.findValues(TERMS).stream())
        .flatMap(term -> StreamSupport.stream(Spliterators.spliteratorUnknownSize(term.fieldNames(), 0), false))
        .toList();
  }

  private JsonNode taxonKeyHeatmapQuery(OccurrenceHeatmapRequest request) throws IOException {
    request.addTaxonKeyFilter("4");
    request.setGeometry("-44, 30, -32, 54");
    request.setZoom(1);
    return MAPPER.readTree(toJson(esHeatmapRequestBuilder.buildHeatmapRequest(request, INDEX)));
  }

  @Test
  public void heatmapWithoutDnaAssistanceTest() throws IOException {
    JsonNode json = taxonKeyHeatmapQuery(new OccurrenceHeatmapRequest());

    assertFalse(json.toString().contains("dnaClassifications"), json.toString());
  }

  @Test
  public void heatmapUseDnaAssistanceTest() throws IOException {
    OccurrenceHeatmapRequest request = new OccurrenceHeatmapRequest();
    request.setUseDnaAssistance(true);

    JsonNode json = taxonKeyHeatmapQuery(request);

    // the taxon key is searched in the regular and the DNA-assisted classifications
    List<String> fields = termFields(json);
    assertTrue(fields.contains(TAXON_KEY_FIELD), json.toString());
    assertTrue(
        fields.contains(dnaTaxonKeyField(UUID.fromString(DEFAULT_CHECKLIST_KEY))), json.toString());
  }

  @Test
  public void heatmapDnaAssistedChecklistKeyTest() throws IOException {
    OccurrenceHeatmapRequest request = new OccurrenceHeatmapRequest();
    request.addParameter(
        OccurrenceSearchParameter.DNA_ASSISTED_CHECKLIST_KEY, DNA_CHECKLIST_KEY.toString());

    JsonNode json = taxonKeyHeatmapQuery(request);

    // only the DNA-assisted classification of that checklist is searched
    List<String> fields = termFields(json);
    assertTrue(fields.contains(dnaTaxonKeyField(DNA_CHECKLIST_KEY)), json.toString());
    assertFalse(fields.contains(TAXON_KEY_FIELD), json.toString());
    // and only the records with a DNA-assisted match in that checklist
    assertTrue(fields.contains("dnaChecklistKey"), json.toString());
  }
}
