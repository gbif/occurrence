package org.gbif.occurrence.search.heatmap.es;

import org.apache.lucene.search.BooleanClause;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.POJONode;
import org.gbif.occurrence.search.es.EsQueryUtils;
import org.gbif.occurrence.search.es.OccurrenceEsField;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequest;
import org.junit.Assert;
import org.junit.Test;

import static org.gbif.occurrence.search.es.EsQueryUtils.*;
import static org.gbif.occurrence.search.heatmap.es.EsHeatmapRequestBuilder.CELL_AGGS;
import static org.gbif.occurrence.search.heatmap.es.EsHeatmapRequestBuilder.HEATMAP_AGGS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EsHeatmapRequestBuilderTest {

  // TODO: adapt tests!!

  @Test
  public void heatmapRequestTest() {
    OccurrenceHeatmapRequest request = new OccurrenceHeatmapRequest();
    request.setGeometry("-44, 30, -32, 54");
    request.setZoom(1);

    ObjectNode json = EsHeatmapRequestBuilder.buildQuery(request);
    System.out.println(json.toString());

    assertEquals(0, json.get(SIZE).asInt());
    assertTrue(json.path(QUERY).path(BOOL).path(FILTER).isArray());
    assertTrue(json.path(QUERY).path(BOOL).path(FILTER).get(0).has(GEO_BOUNDING_BOX));

    // assert bbox
    JsonNode bbox =
        json.path(QUERY)
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(GEO_BOUNDING_BOX)
            .path(OccurrenceEsField.COORDINATE_POINT.getFieldName());
    assertEquals("[-44.0, 54.0]", ((POJONode) bbox.path("top_left")).asText());
    assertEquals("[-32.0, 30.0]", ((POJONode) bbox.path("bottom_right")).asText());

    // aggs
    assertTrue(json.path(AGGS).path(HEATMAP_AGGS).has(GEOHASH_GRID));
    assertEquals(
        OccurrenceEsField.COORDINATE_POINT.getFieldName(),
        json.path(AGGS).path(HEATMAP_AGGS).path(GEOHASH_GRID).get(FIELD).asText());
    assertEquals(1, json.path(AGGS).path(HEATMAP_AGGS).path(GEOHASH_GRID).get(PRECISION).asInt());

    assertTrue(json.path(AGGS).path(HEATMAP_AGGS).path(AGGS).path(CELL_AGGS).has(GEO_BOUNDS));
    assertEquals(
        OccurrenceEsField.COORDINATE_POINT.getFieldName(),
        json.path(AGGS)
            .path(HEATMAP_AGGS)
            .path(AGGS)
            .path(CELL_AGGS)
            .path(GEO_BOUNDS)
            .get(FIELD)
            .asText());
  }

  @Test
  public void heatmapRequestFilteredTest() {
    OccurrenceHeatmapRequest request = new OccurrenceHeatmapRequest();
    request.addTaxonKeyFilter(4);
    request.setGeometry("-44, 30, -32, 54");
    request.setZoom(1);

    ObjectNode json = EsHeatmapRequestBuilder.buildQuery(request);

    assertEquals(0, json.get(SIZE).asInt());
    assertTrue(json.path(QUERY).path(BOOL).path(FILTER).isArray());
    assertTrue(json.path(QUERY).path(BOOL).path(FILTER).get(0).has(GEO_BOUNDING_BOX));

    // taxon key
    assertEquals(
        4,
        json.path(QUERY)
            .path(BOOL)
            .path(FILTER)
            .get(1)
            .path(TERM)
            .get(OccurrenceEsField.TAXA_KEY.getFieldName())
            .asInt());

    // assert bbox
    JsonNode bbox =
        json.path(QUERY)
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(GEO_BOUNDING_BOX)
            .path(OccurrenceEsField.COORDINATE_POINT.getFieldName());
    assertEquals("[-44.0, 54.0]", ((POJONode) bbox.path("top_left")).asText());
    assertEquals("[-32.0, 30.0]", ((POJONode) bbox.path("bottom_right")).asText());

    // aggs
    assertTrue(json.path(AGGS).path(HEATMAP_AGGS).has(GEOHASH_GRID));
    assertEquals(
        OccurrenceEsField.COORDINATE_POINT.getFieldName(),
        json.path(AGGS).path(HEATMAP_AGGS).path(GEOHASH_GRID).get(FIELD).asText());
    assertEquals(1, json.path(AGGS).path(HEATMAP_AGGS).path(GEOHASH_GRID).get(PRECISION).asInt());

    assertTrue(json.path(AGGS).path(HEATMAP_AGGS).path(AGGS).path(CELL_AGGS).has(GEO_BOUNDS));
    assertEquals(
        OccurrenceEsField.COORDINATE_POINT.getFieldName(),
        json.path(AGGS)
            .path(HEATMAP_AGGS)
            .path(AGGS)
            .path(CELL_AGGS)
            .path(GEO_BOUNDS)
            .get(FIELD)
            .asText());
  }
}
