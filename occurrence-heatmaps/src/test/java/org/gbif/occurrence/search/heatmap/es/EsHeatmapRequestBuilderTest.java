package org.gbif.occurrence.search.heatmap.es;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.POJONode;
import org.elasticsearch.action.search.SearchRequest;
import org.gbif.occurrence.search.es.OccurrenceEsField;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequest;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.gbif.occurrence.search.es.EsQueryUtils.*;
import static org.gbif.occurrence.search.heatmap.es.EsHeatmapRequestBuilder.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EsHeatmapRequestBuilderTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String INDEX = "index";

  @Test
  public void heatmapRequestTest() throws IOException {
    OccurrenceHeatmapRequest request = new OccurrenceHeatmapRequest();
    request.setGeometry("-44, 30, -32, 54");
    request.setZoom(1);

    SearchRequest query = EsHeatmapRequestBuilder.buildRequest(request, INDEX);
    JsonNode json = MAPPER.readTree(query.source().toString());
    System.out.println(json.toString());

    assertEquals(0, json.get(SIZE).asInt());

    // aggs
    assertTrue(json.path(AGGREGATIONS).path(BOX_AGGS).path(FILTER).has(GEO_BOUNDING_BOX));

    // assert bbox
    JsonNode bbox =
        json.path(AGGREGATIONS)
            .path(BOX_AGGS)
            .path(FILTER)
            .path(GEO_BOUNDING_BOX)
            .path(OccurrenceEsField.COORDINATE_POINT.getFieldName());
    assertEquals(-44d, bbox.path("top_left").get(0).asDouble(), 0);
    assertEquals(54d, bbox.path("top_left").get(1).asDouble(), 0);
    assertEquals(-32d, bbox.path("bottom_right").get(0).asDouble(), 0);
    assertEquals(30d, bbox.path("bottom_right").get(1).asDouble(), 0);

    // geohash_grid
    assertTrue(
        json.path(AGGREGATIONS)
            .path(BOX_AGGS)
            .path(AGGREGATIONS)
            .path(HEATMAP_AGGS)
            .has(GEOHASH_GRID));
    JsonNode jsonGeohashGrid =
        json.path(AGGREGATIONS)
            .path(BOX_AGGS)
            .path(AGGREGATIONS)
            .path(HEATMAP_AGGS)
            .path(GEOHASH_GRID);
    assertEquals(
        OccurrenceEsField.COORDINATE_POINT.getFieldName(), jsonGeohashGrid.get(FIELD).asText());
    assertEquals(1, jsonGeohashGrid.get(PRECISION).asInt());

    // geo_bounds
    assertTrue(
        json.path(AGGREGATIONS)
            .path(BOX_AGGS)
            .path(AGGREGATIONS)
            .path(HEATMAP_AGGS)
            .path(AGGREGATIONS)
            .path(CELL_AGGS)
            .has(GEO_BOUNDS));
    JsonNode jsonGeobounds =
        json.path(AGGREGATIONS)
            .path(BOX_AGGS)
            .path(AGGREGATIONS)
            .path(HEATMAP_AGGS)
            .path(AGGREGATIONS)
            .path(CELL_AGGS)
            .path(GEO_BOUNDS);
    assertEquals(
        OccurrenceEsField.COORDINATE_POINT.getFieldName(), jsonGeobounds.get(FIELD).asText());
  }

  @Test
  public void heatmapRequestFilteredTest() throws IOException {
    OccurrenceHeatmapRequest request = new OccurrenceHeatmapRequest();
    request.addTaxonKeyFilter(4);
    request.setGeometry("-44, 30, -32, 54");
    request.setZoom(1);

    SearchRequest query = EsHeatmapRequestBuilder.buildRequest(request, INDEX);
    JsonNode json = MAPPER.readTree(query.source().toString());
    System.out.println(json.toString());

    assertEquals(0, json.get(SIZE).asInt());
    assertTrue(json.path(QUERY).path(BOOL).path(FILTER).isArray());
    assertTrue(json.path(QUERY).path(BOOL).path(FILTER).get(0).has(TERM));

    // taxon key
    assertEquals(
        4,
        json.path(QUERY)
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(TERM)
            .get(OccurrenceEsField.TAXA_KEY.getFieldName())
            .get(VALUE)
            .asInt());

    // aggs
    assertTrue(json.path(AGGREGATIONS).path(BOX_AGGS).path(FILTER).has(GEO_BOUNDING_BOX));

    // geohash_grid
    assertTrue(
        json.path(AGGREGATIONS)
            .path(BOX_AGGS)
            .path(AGGREGATIONS)
            .path(HEATMAP_AGGS)
            .has(GEOHASH_GRID));

    // geo_bounds
    assertTrue(
        json.path(AGGREGATIONS)
            .path(BOX_AGGS)
            .path(AGGREGATIONS)
            .path(HEATMAP_AGGS)
            .path(AGGREGATIONS)
            .path(CELL_AGGS)
            .has(GEO_BOUNDS));
  }
}
