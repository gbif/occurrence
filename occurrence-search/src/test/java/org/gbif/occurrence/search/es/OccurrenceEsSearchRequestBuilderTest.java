package org.gbif.occurrence.search.es;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.vocabulary.Country;
import org.junit.Test;

import static org.gbif.occurrence.search.es.EsQueryUtils.*;
import static org.gbif.occurrence.search.es.OccurrenceEsField.TAXON_KEYS_LIST;
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
  public void taxonKeyQueryTest() {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addTaxonKeyFilter(6);

    ObjectNode jsonQuery = EsSearchRequestBuilder.buildQuery(searchRequest);
    System.out.println(jsonQuery);

    assertTrue(jsonQuery.path(BOOL).path(SHOULD).isArray());
    assertEquals(TAXON_KEYS_LIST.size(), jsonQuery.path(BOOL).path(SHOULD).size());
    assertEquals(
        6,
        jsonQuery.path(BOOL).path(SHOULD).findValue(TAXON_KEYS_LIST.get(0).getFieldName()).asInt());
    assertEquals(
        6,
        jsonQuery.path(BOOL).path(SHOULD).findValue(TAXON_KEYS_LIST.get(1).getFieldName()).asInt());
  }

  @Test
  public void mustAndTaxonKeyQueryTest() {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addTaxonKeyFilter(6);
    searchRequest.addCountryFilter(Country.AFGHANISTAN);

    ObjectNode jsonQuery = EsSearchRequestBuilder.buildQuery(searchRequest);
    System.out.println(jsonQuery);

    assertTrue(jsonQuery.path(BOOL).path(SHOULD).isArray());
    assertTrue(jsonQuery.path(BOOL).path(MUST).isArray());
    assertEquals(TAXON_KEYS_LIST.size(), jsonQuery.path(BOOL).path(SHOULD).size());
    assertEquals(1, jsonQuery.path(BOOL).path(MUST).size());
  }

  @Test
  public void geoDistanceQueryTest() {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addDecimalLatitudeFilter(12);
    searchRequest.addDecimalLongitudeFilter(22);

    ObjectNode jsonQuery = EsSearchRequestBuilder.buildQuery(searchRequest);
    System.out.println(jsonQuery);

    assertTrue(jsonQuery.path(BOOL).path(FILTER).has(GEO_DISTANCE));

    JsonNode geoDistance = jsonQuery.path(BOOL).path(FILTER).path(GEO_DISTANCE);
    assertTrue(geoDistance.has(DISTANCE));
    assertEquals(12, geoDistance.path(LOCATION).path(LAT).asDouble(), 0);
    assertEquals(22, geoDistance.path(LOCATION).path(LON).asDouble(), 0);
  }

  @Test
  public void latitudeOnlyQueryTest() {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addDecimalLatitudeFilter(12);

    ObjectNode jsonQuery = EsSearchRequestBuilder.buildQuery(searchRequest);
    System.out.println(jsonQuery);

    assertTrue(jsonQuery.path(BOOL).path(FILTER).has(GEO_BOUNDING_BOX));

    JsonNode geoBoundingBox = jsonQuery.path(BOOL).path(FILTER).path(GEO_BOUNDING_BOX);
    assertTrue(geoBoundingBox.path(LOCATION).has(TOP_LEFT));
    assertTrue(geoBoundingBox.path(LOCATION).has(BOTTOM_RIGHT));

    JsonNode topLeft = geoBoundingBox.path(LOCATION).path(TOP_LEFT);
    JsonNode bottomRight = geoBoundingBox.path(LOCATION).path(BOTTOM_RIGHT);
    assertEquals(12 + MIN_DIFF, topLeft.path(LAT).asDouble(), 0);
    assertEquals(MIN_LON, topLeft.path(LON).asDouble(), 0);
    assertEquals(12 - MIN_DIFF, bottomRight.path(LAT).asDouble(), 0);
    assertEquals(MAX_LON, bottomRight.path(LON).asDouble(), 0);
  }

  @Test
  public void longitudeOnlyQueryTest() {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addDecimalLongitudeFilter(12);

    ObjectNode jsonQuery = EsSearchRequestBuilder.buildQuery(searchRequest);
    System.out.println(jsonQuery);

    assertTrue(jsonQuery.path(BOOL).path(FILTER).has(GEO_BOUNDING_BOX));

    JsonNode geoBoundingBox = jsonQuery.path(BOOL).path(FILTER).path(GEO_BOUNDING_BOX);
    assertTrue(geoBoundingBox.path(LOCATION).has(TOP_LEFT));
    assertTrue(geoBoundingBox.path(LOCATION).has(BOTTOM_RIGHT));

    JsonNode topLeft = geoBoundingBox.path(LOCATION).path(TOP_LEFT);
    JsonNode bottomRight = geoBoundingBox.path(LOCATION).path(BOTTOM_RIGHT);
    assertEquals(12 - MIN_DIFF, topLeft.path(LON).asDouble(), 0);
    assertEquals(MAX_LAT, topLeft.path(LAT).asDouble(), 0);
    assertEquals(12 + MIN_DIFF, bottomRight.path(LON).asDouble(), 0);
    assertEquals(MIN_LAT, bottomRight.path(LAT).asDouble(), 0);
  }

  @Test
  public void geoRangeQueryOnlyLongitudeTest() {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addParameter(OccurrenceSearchParameter.DECIMAL_LONGITUDE, "12,15");

    ObjectNode jsonQuery = EsSearchRequestBuilder.buildQuery(searchRequest);
    System.out.println(jsonQuery);

    assertTrue(jsonQuery.path(BOOL).path(FILTER).has(GEO_BOUNDING_BOX));

    JsonNode geoBoundingBox = jsonQuery.path(BOOL).path(FILTER).path(GEO_BOUNDING_BOX);
    assertTrue(geoBoundingBox.path(LOCATION).has(TOP_LEFT));
    assertTrue(geoBoundingBox.path(LOCATION).has(BOTTOM_RIGHT));

    JsonNode topLeft = geoBoundingBox.path(LOCATION).path(TOP_LEFT);
    JsonNode bottomRight = geoBoundingBox.path(LOCATION).path(BOTTOM_RIGHT);
    assertEquals(12 - MIN_DIFF, topLeft.path(LON).asDouble(), 0);
    assertEquals(MAX_LAT, topLeft.path(LAT).asDouble(), 0);
    assertEquals(15 + MIN_DIFF, bottomRight.path(LON).asDouble(), 0);
    assertEquals(MIN_LAT, bottomRight.path(LAT).asDouble(), 0);
  }

  @Test
  public void geoRangeQueryTest() {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addParameter(OccurrenceSearchParameter.DECIMAL_LONGITUDE, "12,15");
    searchRequest.addParameter(OccurrenceSearchParameter.DECIMAL_LATITUDE, "-10,5");

    ObjectNode jsonQuery = EsSearchRequestBuilder.buildQuery(searchRequest);
    System.out.println(jsonQuery);

    assertTrue(jsonQuery.path(BOOL).path(FILTER).has(GEO_BOUNDING_BOX));

    JsonNode geoBoundingBox = jsonQuery.path(BOOL).path(FILTER).path(GEO_BOUNDING_BOX);
    assertTrue(geoBoundingBox.path(LOCATION).has(TOP_LEFT));
    assertTrue(geoBoundingBox.path(LOCATION).has(BOTTOM_RIGHT));

    JsonNode topLeft = geoBoundingBox.path(LOCATION).path(TOP_LEFT);
    JsonNode bottomRight = geoBoundingBox.path(LOCATION).path(BOTTOM_RIGHT);
    assertEquals(12 - MIN_DIFF, topLeft.path(LON).asDouble(), 0);
    assertEquals(5 + MIN_DIFF, topLeft.path(LAT).asDouble(), 0);
    assertEquals(15 + MIN_DIFF, bottomRight.path(LON).asDouble(), 0);
    assertEquals(-10 - MIN_DIFF, bottomRight.path(LAT).asDouble(), 0);
  }
}
