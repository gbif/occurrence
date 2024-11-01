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

import org.gbif.occurrence.search.es.EsField;
import org.gbif.occurrence.search.es.OccurrenceEsField;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequest;

import java.io.IOException;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import org.elasticsearch.action.search.SearchRequest;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import static org.gbif.occurrence.search.es.EsQueryUtils.*;
import static org.gbif.occurrence.search.heatmap.es.EsHeatmapRequestBuilder.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests for ElasticSearch heatmap request builders. */
public class EsHeatmapRequestBuilderTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String INDEX = "index";
  private final EsHeatmapRequestBuilder esHeatmapRequestBuilder =
      new EsHeatmapRequestBuilder(OccurrenceEsField.buildFieldMapper(), null);

  @Test
  public void heatmapRequestTest() throws IOException {
    OccurrenceHeatmapRequest request = new OccurrenceHeatmapRequest();
    request.setGeometry("-44, 30, -32, 54");
    request.setZoom(1);

    SearchRequest query = esHeatmapRequestBuilder.buildRequest(request, INDEX);
    JsonNode json = MAPPER.readTree(query.source().toString());

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
    assertEquals(-44d, bbox.path("top_left").get(0).asDouble(), 0);
    assertEquals(54d, bbox.path("top_left").get(1).asDouble(), 0);
    assertEquals(-32d, bbox.path("bottom_right").get(0).asDouble(), 0);
    assertEquals(30d, bbox.path("bottom_right").get(1).asDouble(), 0);

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
  private static Optional<String> findTermFilter(JsonNode node, EsField field) {
    ArrayNode arrayNode =
        (ArrayNode) node.path(QUERY).path(BOOL).path(FILTER).get(2).path(BOOL).path(FILTER);
    return StreamSupport.stream(
            Spliterators.spliterator(arrayNode.elements(), 2, Spliterator.ORDERED), false)
        .filter(termNode -> termNode.path(TERM).has(field.getSearchFieldName()))
        .map(termNode -> termNode.path(TERM).get(field.getSearchFieldName()).get(VALUE).asText())
        .findFirst();
  }

  @Test
  public void heatmapRequestFilteredTest() throws IOException {
    OccurrenceHeatmapRequest request = new OccurrenceHeatmapRequest();
    request.addTaxonKeyFilter(4);
    request.setGeometry("-44, 30, -32, 54");
    request.setZoom(1);

    SearchRequest query = esHeatmapRequestBuilder.buildRequest(request, INDEX);
    JsonNode json = MAPPER.readTree(query.source().toString());

    assertEquals(0, json.get(SIZE).asInt());
    assertTrue(json.path(QUERY).path(BOOL).path(FILTER).isArray());
    assertTrue(json.path(QUERY).path(BOOL).path(FILTER).get(1).has(TERM));

    // taxon key
    Optional<String> taxaValue = findTermFilter(json, OccurrenceEsField.TAXON_KEY);

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
}
