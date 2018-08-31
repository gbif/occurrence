package org.gbif.occurrence.search.es;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.vocabulary.Country;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.occurrence.search.es.EsQueryUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OccurrenceEsSearchRequestBuilderTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OccurrenceEsSearchRequestBuilderTest.class);

//  @Test
//  public void matchQueryTest() {
//    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
//    searchRequest.addKingdomKeyFilter(6);
//
//    ObjectNode jsonQuery =
//        EsSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalStateException::new);
//    LOG.debug("Query: {}", jsonQuery);
//
//    assertTrue(jsonQuery.path(BOOL).path(MUST).isArray());
//    assertEquals(1, jsonQuery.path(BOOL).path(MUST).size());
//    assertEquals(
//        6,
//        jsonQuery
//            .path(BOOL)
//            .path(MUST)
//            .findValue(OccurrenceEsField.KINGDOM_KEY.getFieldName())
//            .asInt());
//  }
//
//  @Test
//  public void multiMatchQueryTest() {
//    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
//    searchRequest.addYearFilter(1999);
//    searchRequest.addCountryFilter(Country.AFGHANISTAN);
//
//    ObjectNode jsonQuery =
//        EsSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalStateException::new);
//    LOG.debug("Query: {}", jsonQuery);
//
//    assertTrue(jsonQuery.path(BOOL).path(MUST).isArray());
//    assertEquals(2, jsonQuery.path(BOOL).path(MUST).size());
//    assertEquals(
//        1999,
//        jsonQuery.path(BOOL).path(MUST).findValue(OccurrenceEsField.YEAR.getFieldName()).asInt());
//    assertEquals(
//        Country.AFGHANISTAN.getIso2LetterCode(),
//        jsonQuery
//            .path(BOOL)
//            .path(MUST)
//            .findValue(OccurrenceEsField.COUNTRY_CODE.getFieldName())
//            .asText());
//  }
//
//  @Test
//  public void multiTermQueryTest() {
//    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
//    searchRequest.addMonthFilter(1);
//    searchRequest.addMonthFilter(2);
//
//    ObjectNode jsonQuery =
//        EsSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalStateException::new);
//    LOG.debug("Query: {}", jsonQuery);
//
//    assertTrue(jsonQuery.path(BOOL).path(MUST).isArray());
//    assertTrue(jsonQuery.path(BOOL).path(MUST).get(0).has(TERMS));
//    assertEquals(
//        2,
//        jsonQuery
//            .path(BOOL)
//            .path(MUST)
//            .get(0)
//            .path(TERMS)
//            .path(OccurrenceEsField.MONTH.getFieldName())
//            .size());
//  }
//
//  @Test
//  public void rangeQueryTest() {
//    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
//    searchRequest.addParameter(OccurrenceSearchParameter.DECIMAL_LATITUDE, "12, 25");
//
//    ObjectNode jsonQuery =
//        EsSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalStateException::new);
//    LOG.debug("Query: {}", jsonQuery);
//
//    assertTrue(jsonQuery.path(BOOL).path(MUST).isArray());
//    JsonNode latitudeNode =
//        jsonQuery
//            .path(BOOL)
//            .path(MUST)
//            .findValue(RANGE)
//            .path(OccurrenceEsField.LATITUDE.getFieldName());
//    assertEquals(12, latitudeNode.path(GTE).asDouble(), 0);
//    assertEquals(25, latitudeNode.path(LTE).asDouble(), 0);
//  }
//
//  @Test
//  public void polygonQueryTest() {
//    final String polygon = "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))";
//    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
//    searchRequest.addGeometryFilter(polygon);
//
//    ObjectNode jsonQuery =
//        EsSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalStateException::new);
//    LOG.debug("Query: {}", jsonQuery);
//
//    assertTrue(
//        jsonQuery
//            .path(BOOL)
//            .path(FILTER)
//            .get(0)
//            .path(GEO_SHAPE)
//            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
//            .has(SHAPE));
//    JsonNode shape =
//        jsonQuery
//            .path(BOOL)
//            .path(FILTER)
//            .get(0)
//            .path(GEO_SHAPE)
//            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
//            .path(SHAPE);
//    assertEquals("POLYGON", shape.get(TYPE).asText());
//    assertTrue(shape.get(COORDINATES).isArray());
//    assertEquals(5, shape.get(COORDINATES).get(0).size());
//  }
//
//  @Test
//  public void polygonWithHoleQueryTest() {
//    final String polygonWithHole =
//        "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0), (100.2 0.2, 100.8 0.2, 100.8 0.8, 100.2 0.8, 100.2 0.2))";
//    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
//    searchRequest.addGeometryFilter(polygonWithHole);
//
//    ObjectNode jsonQuery =
//        EsSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalStateException::new);
//    LOG.debug("Query: {}", jsonQuery);
//
//    assertTrue(
//        jsonQuery
//            .path(BOOL)
//            .path(FILTER)
//            .get(0)
//            .path(GEO_SHAPE)
//            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
//            .has(SHAPE));
//    JsonNode shape =
//        jsonQuery
//            .path(BOOL)
//            .path(FILTER)
//            .get(0)
//            .path(GEO_SHAPE)
//            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
//            .path(SHAPE);
//    assertEquals("POLYGON", shape.get(TYPE).asText());
//    assertTrue(shape.get(COORDINATES).isArray());
//    assertEquals(2, shape.get(COORDINATES).size());
//    assertEquals(5, shape.get(COORDINATES).get(0).size());
//    assertEquals(5, shape.get(COORDINATES).get(1).size());
//  }
//
//  @Test
//  public void multipolygonQueryTest() {
//    final String multipolygon =
//        "MULTIPOLYGON (((102.0 2.0, 103.0 2.0, 103.0 3.0, 102.0 3.0, 102.0 2.0)), ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0), (100.2 0.2, 100.8 0.2, 100.8 0.8, 100.2 0.8, 100.2 0.2)))";
//    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
//    searchRequest.addGeometryFilter(multipolygon);
//
//    ObjectNode jsonQuery =
//        EsSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalStateException::new);
//    LOG.debug("Query: {}", jsonQuery);
//
//    assertTrue(
//        jsonQuery
//            .path(BOOL)
//            .path(FILTER)
//            .get(0)
//            .path(GEO_SHAPE)
//            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
//            .has(SHAPE));
//    JsonNode shape =
//        jsonQuery
//            .path(BOOL)
//            .path(FILTER)
//            .get(0)
//            .path(GEO_SHAPE)
//            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
//            .path(SHAPE);
//    assertEquals("MULTIPOLYGON", shape.get(TYPE).asText());
//    assertTrue(shape.get(COORDINATES).isArray());
//    assertEquals(2, shape.get(COORDINATES).size());
//    assertEquals(1, shape.get(COORDINATES).get(0).size());
//    assertEquals(5, shape.get(COORDINATES).get(0).get(0).size());
//    assertEquals(2, shape.get(COORDINATES).get(1).size());
//    assertEquals(5, shape.get(COORDINATES).get(1).get(0).size());
//    assertEquals(5, shape.get(COORDINATES).get(1).get(1).size());
//  }
//
//  @Test
//  public void linestringQueryTest() {
//    final String linestring = "LINESTRING (-77.03653 38.897676, -77.009051 38.889939)";
//    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
//    searchRequest.addGeometryFilter(linestring);
//
//    ObjectNode jsonQuery =
//        EsSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalStateException::new);
//    LOG.debug("Query: {}", jsonQuery);
//
//    assertTrue(
//        jsonQuery
//            .path(BOOL)
//            .path(FILTER)
//            .get(0)
//            .path(GEO_SHAPE)
//            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
//            .has(SHAPE));
//    JsonNode shape =
//        jsonQuery
//            .path(BOOL)
//            .path(FILTER)
//            .get(0)
//            .path(GEO_SHAPE)
//            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
//            .path(SHAPE);
//    assertEquals("LINESTRING", shape.get(TYPE).asText());
//    assertTrue(shape.get(COORDINATES).isArray());
//    assertEquals(2, shape.get(COORDINATES).size());
//    assertEquals(-77.03653, shape.get(COORDINATES).get(0).get(0).asDouble(), 0);
//  }
//
//  @Test
//  public void linearringQueryTest() {
//    final String linearring = "LINEARRING (12 12, 14 10, 13 14, 12 12)";
//    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
//    searchRequest.addGeometryFilter(linearring);
//
//    ObjectNode jsonQuery =
//        EsSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalStateException::new);
//    LOG.debug("Query: {}", jsonQuery);
//
//    assertTrue(
//        jsonQuery
//            .path(BOOL)
//            .path(FILTER)
//            .get(0)
//            .path(GEO_SHAPE)
//            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
//            .has(SHAPE));
//    JsonNode shape =
//        jsonQuery
//            .path(BOOL)
//            .path(FILTER)
//            .get(0)
//            .path(GEO_SHAPE)
//            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
//            .path(SHAPE);
//    assertEquals("LINESTRING", shape.get(TYPE).asText());
//    assertTrue(shape.get(COORDINATES).isArray());
//    assertEquals(4, shape.get(COORDINATES).size());
//    assertEquals(12, shape.get(COORDINATES).get(0).get(0).asDouble(), 0);
//  }
//
//  @Test
//  public void pointQueryTest() {
//    final String point = "POINT (-77.03653 38.897676)";
//    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
//    searchRequest.addGeometryFilter(point);
//
//    ObjectNode jsonQuery =
//        EsSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalStateException::new);
//    LOG.debug("Query: {}", jsonQuery);
//
//    assertTrue(
//        jsonQuery
//            .path(BOOL)
//            .path(FILTER)
//            .get(0)
//            .path(GEO_SHAPE)
//            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
//            .has(SHAPE));
//    JsonNode shape =
//        jsonQuery
//            .path(BOOL)
//            .path(FILTER)
//            .get(0)
//            .path(GEO_SHAPE)
//            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
//            .path(SHAPE);
//    assertEquals("POINT", shape.get(TYPE).asText());
//    assertTrue(shape.get(COORDINATES).isArray());
//    assertEquals(2, shape.get(COORDINATES).size());
//    assertEquals(-77.03653d, shape.get(COORDINATES).get(0).asDouble(), 0);
//  }
}
