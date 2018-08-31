package org.gbif.occurrence.search.es;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.vocabulary.Country;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.gbif.occurrence.search.es.EsQueryUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OccurrenceEsSearchRequestBuilderTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(OccurrenceEsSearchRequestBuilderTest.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void matchQueryTest() throws IOException {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addKingdomKeyFilter(6);

    QueryBuilder query =
        EsSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(jsonQuery.path(BOOL).path(FILTER).isArray());
    assertEquals(1, jsonQuery.path(BOOL).path(FILTER).size());
    assertEquals(
        6,
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .findValue(OccurrenceEsField.KINGDOM_KEY.getFieldName())
            .get(VALUE)
            .asInt());
  }

  @Test
  public void multiMatchQueryTest() throws IOException {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addYearFilter(1999);
    searchRequest.addCountryFilter(Country.AFGHANISTAN);

    QueryBuilder query =
        EsSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(jsonQuery.path(BOOL).path(FILTER).isArray());
    assertEquals(2, jsonQuery.path(BOOL).path(FILTER).size());
    assertEquals(
        1999,
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .findValue(OccurrenceEsField.YEAR.getFieldName())
            .get(VALUE)
            .asInt());
    assertEquals(
        Country.AFGHANISTAN.getIso2LetterCode(),
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .findValue(OccurrenceEsField.COUNTRY_CODE.getFieldName())
            .get(VALUE)
            .asText());
  }

  @Test
  public void multiTermQueryTest() throws IOException {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addMonthFilter(1);
    searchRequest.addMonthFilter(2);

    QueryBuilder query =
        EsSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(jsonQuery.path(BOOL).path(FILTER).isArray());
    assertTrue(jsonQuery.path(BOOL).path(FILTER).get(0).has(TERMS));
    assertEquals(
        2,
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(TERMS)
            .path(OccurrenceEsField.MONTH.getFieldName())
            .size());
  }

  @Test
  public void rangeQueryTest() throws IOException {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addParameter(OccurrenceSearchParameter.DECIMAL_LATITUDE, "12, 25");

    QueryBuilder query =
        EsSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(jsonQuery.path(BOOL).path(FILTER).isArray());
    JsonNode latitudeNode =
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .findValue(RANGE)
            .path(OccurrenceEsField.LATITUDE.getFieldName());
    assertEquals(12, latitudeNode.path(FROM).asDouble(), 0);
    assertEquals(25, latitudeNode.path(TO).asDouble(), 0);
  }

  @Test
  public void polygonQueryTest() throws IOException {
    final String polygon = "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))";
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addGeometryFilter(polygon);

    QueryBuilder query =
        EsSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
            .has(SHAPE));
    JsonNode shape =
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
            .path(SHAPE);
    assertEquals("polygon", shape.get(TYPE).asText());
    assertTrue(shape.get(COORDINATES).isArray());
    assertEquals(5, shape.get(COORDINATES).get(0).size());
  }

  @Test
  public void polygonWithHoleQueryTest() throws IOException {
    final String polygonWithHole =
        "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0), (100.2 0.2, 100.8 0.2, 100.8 0.8, 100.2 0.8, 100.2 0.2))";
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addGeometryFilter(polygonWithHole);

    QueryBuilder query =
        EsSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
            .has(SHAPE));
    JsonNode shape =
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
            .path(SHAPE);
    assertEquals("polygon", shape.get(TYPE).asText());
    assertTrue(shape.get(COORDINATES).isArray());
    assertEquals(2, shape.get(COORDINATES).size());
    assertEquals(5, shape.get(COORDINATES).get(0).size());
    assertEquals(5, shape.get(COORDINATES).get(1).size());
  }

  @Test
  public void multipolygonQueryTest() throws IOException {
    final String multipolygon =
        "MULTIPOLYGON (((102.0 2.0, 103.0 2.0, 103.0 3.0, 102.0 3.0, 102.0 2.0)), ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0), (100.2 0.2, 100.8 0.2, 100.8 0.8, 100.2 0.8, 100.2 0.2)))";
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addGeometryFilter(multipolygon);

    QueryBuilder query =
        EsSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
            .has(SHAPE));
    JsonNode shape =
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
            .path(SHAPE);
    assertEquals("multipolygon", shape.get(TYPE).asText());
    assertTrue(shape.get(COORDINATES).isArray());
    assertEquals(2, shape.get(COORDINATES).size());
    assertEquals(1, shape.get(COORDINATES).get(0).size());
    assertEquals(5, shape.get(COORDINATES).get(0).get(0).size());
    assertEquals(2, shape.get(COORDINATES).get(1).size());
    assertEquals(5, shape.get(COORDINATES).get(1).get(0).size());
    assertEquals(5, shape.get(COORDINATES).get(1).get(1).size());
  }

  @Test
  public void linestringQueryTest() throws IOException {
    final String linestring = "LINESTRING (-77.03653 38.897676, -77.009051 38.889939)";
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addGeometryFilter(linestring);

    QueryBuilder query =
        EsSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
            .has(SHAPE));
    JsonNode shape =
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
            .path(SHAPE);
    assertEquals("linestring", shape.get(TYPE).asText());
    assertTrue(shape.get(COORDINATES).isArray());
    assertEquals(2, shape.get(COORDINATES).size());
    assertEquals(-77.03653, shape.get(COORDINATES).get(0).get(0).asDouble(), 0);
  }

  @Test
  public void linearringQueryTest() throws IOException {
    final String linearring = "LINEARRING (12 12, 14 10, 13 14, 12 12)";
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addGeometryFilter(linearring);

    QueryBuilder query =
        EsSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
            .has(SHAPE));
    JsonNode shape =
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
            .path(SHAPE);
    assertEquals("linestring", shape.get(TYPE).asText());
    assertTrue(shape.get(COORDINATES).isArray());
    assertEquals(4, shape.get(COORDINATES).size());
    assertEquals(12, shape.get(COORDINATES).get(0).get(0).asDouble(), 0);
  }

  @Test
  public void pointQueryTest() throws IOException {
    final String point = "POINT (-77.03653 38.897676)";
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addGeometryFilter(point);

    QueryBuilder query =
        EsSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    LOG.debug("Query: {}", jsonQuery);

    assertTrue(
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
            .has(SHAPE));
    JsonNode shape =
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .get(0)
            .path(BOOL)
            .path(SHOULD)
            .get(0)
            .path(GEO_SHAPE)
            .path(OccurrenceEsField.COORDINATE_SHAPE.getFieldName())
            .path(SHAPE);
    assertEquals("point", shape.get(TYPE).asText());
    assertTrue(shape.get(COORDINATES).isArray());
    assertEquals(2, shape.get(COORDINATES).size());
    assertEquals(-77.03653d, shape.get(COORDINATES).get(0).asDouble(), 0);
  }
}
