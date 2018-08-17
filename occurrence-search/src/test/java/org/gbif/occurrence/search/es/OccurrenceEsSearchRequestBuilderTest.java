package org.gbif.occurrence.search.es;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.vocabulary.Country;
import org.junit.Test;

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

    assertTrue(
        jsonQuery.path(BOOL).path(MUST).isArray());
    JsonNode latitudeNode =
        jsonQuery.path(BOOL).path(MUST).findValue(RANGE).path(OccurrenceEsField.LATITUDE.getFieldName());
    assertEquals(12, latitudeNode.path(GTE).asDouble(), 0);
    assertEquals(25, latitudeNode.path(LTE).asDouble(), 0);
  }

  //  @Test
  //  public void geoDistanceQueryTest() {
  //    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
  //    searchRequest.addDecimalLatitudeFilter(12);
  //    searchRequest.addDecimalLongitudeFilter(22);
  //
  //    ObjectNode jsonQuery = EsSearchRequestBuilder.buildQuery(searchRequest);
  //    System.out.println(jsonQuery);
  //
  //    assertTrue(jsonQuery.path(BOOL).path(FILTER).has(GEO_DISTANCE));
  //
  //    JsonNode geoDistance = jsonQuery.path(BOOL).path(FILTER).path(GEO_DISTANCE);
  //    assertTrue(geoDistance.has(DISTANCE));
  //    assertEquals(12, geoDistance.path(COORDINATE).path(LAT).asDouble(), 0);
  //    assertEquals(22, geoDistance.path(COORDINATE).path(LON).asDouble(), 0);
  //  }
  //
  //  @Test
  //  public void latitudeOnlyQueryTest() {
  //    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
  //    searchRequest.addDecimalLatitudeFilter(12);
  //
  //    ObjectNode jsonQuery = EsSearchRequestBuilder.buildQuery(searchRequest);
  //    System.out.println(jsonQuery);
  //
  //    assertTrue(jsonQuery.path(BOOL).path(FILTER).has(GEO_BOUNDING_BOX));
  //
  //    JsonNode geoBoundingBox = jsonQuery.path(BOOL).path(FILTER).path(GEO_BOUNDING_BOX);
  //    assertTrue(geoBoundingBox.path(COORDINATE).has(TOP_LEFT));
  //    assertTrue(geoBoundingBox.path(COORDINATE).has(BOTTOM_RIGHT));
  //
  //    JsonNode topLeft = geoBoundingBox.path(COORDINATE).path(TOP_LEFT);
  //    JsonNode bottomRight = geoBoundingBox.path(COORDINATE).path(BOTTOM_RIGHT);
  //    assertEquals(12 + MIN_DIFF, topLeft.path(LAT).asDouble(), 0);
  //    assertEquals(MIN_LON, topLeft.path(LON).asDouble(), 0);
  //    assertEquals(12 - MIN_DIFF, bottomRight.path(LAT).asDouble(), 0);
  //    assertEquals(MAX_LON, bottomRight.path(LON).asDouble(), 0);
  //  }

  //  @Test
  //  public void longitudeOnlyQueryTest() {
  //    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
  //    searchRequest.addDecimalLongitudeFilter(12);
  //
  //    ObjectNode jsonQuery = EsSearchRequestBuilder.buildQuery(searchRequest);
  //    System.out.println(jsonQuery);
  //
  //    assertTrue(jsonQuery.path(BOOL).path(FILTER).has(GEO_BOUNDING_BOX));
  //
  //    JsonNode geoBoundingBox = jsonQuery.path(BOOL).path(FILTER).path(GEO_BOUNDING_BOX);
  //    assertTrue(geoBoundingBox.path(COORDINATE).has(TOP_LEFT));
  //    assertTrue(geoBoundingBox.path(COORDINATE).has(BOTTOM_RIGHT));
  //
  //    JsonNode topLeft = geoBoundingBox.path(COORDINATE).path(TOP_LEFT);
  //    JsonNode bottomRight = geoBoundingBox.path(COORDINATE).path(BOTTOM_RIGHT);
  //    assertEquals(12 - MIN_DIFF, topLeft.path(LON).asDouble(), 0);
  //    assertEquals(MAX_LAT, topLeft.path(LAT).asDouble(), 0);
  //    assertEquals(12 + MIN_DIFF, bottomRight.path(LON).asDouble(), 0);
  //    assertEquals(MIN_LAT, bottomRight.path(LAT).asDouble(), 0);
  //  }
  //
  //  @Test
  //  public void geoRangeQueryOnlyLongitudeTest() {
  //    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
  //    searchRequest.addParameter(OccurrenceSearchParameter.DECIMAL_LONGITUDE, "12,15");
  //
  //    ObjectNode jsonQuery = EsSearchRequestBuilder.buildQuery(searchRequest);
  //    System.out.println(jsonQuery);
  //
  //    assertTrue(jsonQuery.path(BOOL).path(FILTER).has(GEO_BOUNDING_BOX));
  //
  //    JsonNode geoBoundingBox = jsonQuery.path(BOOL).path(FILTER).path(GEO_BOUNDING_BOX);
  //    assertTrue(geoBoundingBox.path(COORDINATE).has(TOP_LEFT));
  //    assertTrue(geoBoundingBox.path(COORDINATE).has(BOTTOM_RIGHT));
  //
  //    JsonNode topLeft = geoBoundingBox.path(COORDINATE).path(TOP_LEFT);
  //    JsonNode bottomRight = geoBoundingBox.path(COORDINATE).path(BOTTOM_RIGHT);
  //    assertEquals(12 - MIN_DIFF, topLeft.path(LON).asDouble(), 0);
  //    assertEquals(MAX_LAT, topLeft.path(LAT).asDouble(), 0);
  //    assertEquals(15 + MIN_DIFF, bottomRight.path(LON).asDouble(), 0);
  //    assertEquals(MIN_LAT, bottomRight.path(LAT).asDouble(), 0);
  //  }
  //
  //  @Test
  //  public void geoRangeQueryTest() {
  //    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
  //    searchRequest.addParameter(OccurrenceSearchParameter.DECIMAL_LONGITUDE, "12,15");
  //    searchRequest.addParameter(OccurrenceSearchParameter.DECIMAL_LATITUDE, "-10,5");
  //
  //    ObjectNode jsonQuery = EsSearchRequestBuilder.buildQuery(searchRequest);
  //    System.out.println(jsonQuery);
  //
  //    assertTrue(jsonQuery.path(BOOL).path(FILTER).has(GEO_BOUNDING_BOX));
  //
  //    JsonNode geoBoundingBox = jsonQuery.path(BOOL).path(FILTER).path(GEO_BOUNDING_BOX);
  //    assertTrue(geoBoundingBox.path(COORDINATE).has(TOP_LEFT));
  //    assertTrue(geoBoundingBox.path(COORDINATE).has(BOTTOM_RIGHT));
  //
  //    JsonNode topLeft = geoBoundingBox.path(COORDINATE).path(TOP_LEFT);
  //    JsonNode bottomRight = geoBoundingBox.path(COORDINATE).path(BOTTOM_RIGHT);
  //    assertEquals(12 - MIN_DIFF, topLeft.path(LON).asDouble(), 0);
  //    assertEquals(5 + MIN_DIFF, topLeft.path(LAT).asDouble(), 0);
  //    assertEquals(15 + MIN_DIFF, bottomRight.path(LON).asDouble(), 0);
  //    assertEquals(-10 - MIN_DIFF, bottomRight.path(LAT).asDouble(), 0);
  //  }
}
