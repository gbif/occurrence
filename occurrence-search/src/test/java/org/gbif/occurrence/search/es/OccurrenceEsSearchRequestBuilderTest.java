package org.gbif.occurrence.search.es;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.vocabulary.Country;
import org.junit.Test;

import javax.validation.constraints.AssertTrue;

import static org.gbif.occurrence.search.es.EsQueryUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OccurrenceEsSearchRequestBuilderTest {

  @Test
  public void matchQueryTest() {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addKingdomKeyFilter(6);

    ObjectNode jsonQuery = EsSearchRequestBuilder.buildQuery(searchRequest);
    System.out.println(jsonQuery);

    assertTrue(jsonQuery.path(BOOL).path(MUST).isArray());
    assertEquals(1, jsonQuery.path(BOOL).path(MUST).size());
    assertEquals(
        6,
        jsonQuery
            .path(BOOL)
            .path(MUST)
            .findValue(OccurrenceEsField.KINGDOM_KEY.getFieldName())
            .asInt());
  }

  @Test
  public void multiMatchQueryTest() {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addYearFilter(1999);
    searchRequest.addCountryFilter(Country.AFGHANISTAN);

    ObjectNode jsonQuery = EsSearchRequestBuilder.buildQuery(searchRequest);
    System.out.println(jsonQuery);

    assertTrue(jsonQuery.path(BOOL).path(MUST).isArray());
    assertEquals(2, jsonQuery.path(BOOL).path(MUST).size());
    assertEquals(
        1999,
        jsonQuery.path(BOOL).path(MUST).findValue(OccurrenceEsField.YEAR.getFieldName()).asInt());
    assertEquals(
        Country.AFGHANISTAN.getIso2LetterCode(),
        jsonQuery
            .path(BOOL)
            .path(MUST)
            .findValue(OccurrenceEsField.COUNTRY_CODE.getFieldName())
            .asText());
  }

  @Test
  public void rangeQueryTest() {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addParameter(OccurrenceSearchParameter.DECIMAL_LATITUDE, "12, 25");

    ObjectNode jsonQuery = EsSearchRequestBuilder.buildQuery(searchRequest);
    System.out.println(jsonQuery);

    assertTrue(jsonQuery.path(BOOL).path(MUST).isArray());
    JsonNode latitudeNode =
        jsonQuery
            .path(BOOL)
            .path(MUST)
            .findValue(RANGE)
            .path(OccurrenceEsField.LATITUDE.getFieldName());
    assertEquals(12, latitudeNode.path(GTE).asDouble(), 0);
    assertEquals(25, latitudeNode.path(LTE).asDouble(), 0);
  }

  @Test
  public void polygonQueryTest() {
    String wkt =
        "MULTIPOLYGON (((102.0 2.0, 103.0 2.0, 103.0 3.0, 102.0 3.0, 102.0 2.0)), ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0), (100.2 0.2, 100.8 0.2, 100.8 0.8, 100.2 0.8, 100.2 0.2)))";
    final String polygon = "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))";

    ObjectNode json = EsSearchRequestBuilder.buildGeoShapeQuery(polygon);
    System.out.println(json);

    assertTrue(json.path(GEO_SHAPE).path(OccurrenceEsField.COORDINATE.getFieldName()).has(SHAPE));
    JsonNode shape =
        json.path(GEO_SHAPE).path(OccurrenceEsField.COORDINATE.getFieldName()).path(SHAPE);
    assertEquals("POLYGON", shape.get(TYPE).asText());
    assertTrue(shape.get(COORDINATES).isArray());
    assertEquals(5, shape.get(COORDINATES).get(0).size());

    String polygonWithHole =
        "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0), (100.2 0.2, 100.8 0.2, 100.8 0.8, 100.2 0.8, 100.2 0.2))";
  }

  @Test
  public void polygonWithHoleQueryTest() {
    final String polygonWithHole =
        "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0), (100.2 0.2, 100.8 0.2, 100.8 0.8, 100.2 0.8, 100.2 0.2))";

    ObjectNode json = EsSearchRequestBuilder.buildGeoShapeQuery(polygonWithHole);
    System.out.println(json);

    assertTrue(json.path(GEO_SHAPE).path(OccurrenceEsField.COORDINATE.getFieldName()).has(SHAPE));
    JsonNode shape =
        json.path(GEO_SHAPE).path(OccurrenceEsField.COORDINATE.getFieldName()).path(SHAPE);
    assertEquals("POLYGON", shape.get(TYPE).asText());
    assertTrue(shape.get(COORDINATES).isArray());
    assertEquals(2, shape.get(COORDINATES).size());
    assertEquals(5, shape.get(COORDINATES).get(0).size());
    assertEquals(5, shape.get(COORDINATES).get(1).size());
  }

  @Test
  public void multipolygonQueryTest() {
    final String multipolygon =
        "MULTIPOLYGON (((102.0 2.0, 103.0 2.0, 103.0 3.0, 102.0 3.0, 102.0 2.0)), ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0), (100.2 0.2, 100.8 0.2, 100.8 0.8, 100.2 0.8, 100.2 0.2)))";

    ObjectNode json = EsSearchRequestBuilder.buildGeoShapeQuery(multipolygon);
    System.out.println(json);

    assertTrue(json.path(GEO_SHAPE).path(OccurrenceEsField.COORDINATE.getFieldName()).has(SHAPE));
    JsonNode shape =
        json.path(GEO_SHAPE).path(OccurrenceEsField.COORDINATE.getFieldName()).path(SHAPE);
    assertEquals("MULTIPOLYGON", shape.get(TYPE).asText());
    assertTrue(shape.get(COORDINATES).isArray());
    assertEquals(2, shape.get(COORDINATES).size());
    assertEquals(1, shape.get(COORDINATES).get(0).size());
    assertEquals(5, shape.get(COORDINATES).get(0).get(0).size());
    assertEquals(2, shape.get(COORDINATES).get(1).size());
    assertEquals(5, shape.get(COORDINATES).get(1).get(0).size());
    assertEquals(5, shape.get(COORDINATES).get(1).get(1).size());
  }
}
