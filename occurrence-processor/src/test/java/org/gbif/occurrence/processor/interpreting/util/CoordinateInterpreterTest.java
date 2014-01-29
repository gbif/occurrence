package org.gbif.occurrence.processor.interpreting.util;

import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.occurrence.processor.interpreting.result.CoordinateCountry;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Ignore("requires live webservice")
public class CoordinateInterpreterTest {

  @Test
  public void testLookupCountryMatch() {
    Double lat = 43.65;
    Double lng = -79.40;
    Country country = Country.CANADA;
    ParseResult<CoordinateCountry> result =
      CoordinateInterpreter.interpretCoordinates(lat.toString(), lng.toString(), country);
    assertEquals(lat, result.getPayload().getLatitude().doubleValue(), 0.0001);
    assertEquals(lng, result.getPayload().getLongitude().doubleValue(), 0.0001);
    assertEquals(country, result.getPayload().getCountry());
    Assert.assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void testLookupCountryEmpty() {
    Double lat = -37.78;
    Double lng = 144.97;
    ParseResult<CoordinateCountry> result =
      CoordinateInterpreter.interpretCoordinates(lat.toString(), lng.toString(), null);
    assertEquals(lat, result.getPayload().getLatitude().doubleValue(), 0.0001);
    assertEquals(lng, result.getPayload().getLongitude().doubleValue(), 0.0001);
    assertEquals(Country.AUSTRALIA, result.getPayload().getCountry());
    assertTrue(result.getIssues().contains(OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES));
  }

  @Test
  public void testLookupCountryMisMatch() {
    Double lat = 55.68;
    Double lng = 12.57;
    Country country = Country.SWEDEN;
    ParseResult<CoordinateCountry> result =
      CoordinateInterpreter.interpretCoordinates(lat.toString(), lng.toString(), country);
    assertEquals(lat, result.getPayload().getLatitude().doubleValue(), 0.0001);
    assertEquals(lng, result.getPayload().getLongitude().doubleValue(), 0.0001);
    assertEquals(country, result.getPayload().getCountry());
    assertTrue(result.getIssues().contains(OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH));
  }

  @Test
  public void testLookupIllegalCoord() {
    Double lat = 200d;
    Double lng = 200d;
    Country country = null; // "asdf"
    ParseResult<CoordinateCountry> result =
      CoordinateInterpreter.interpretCoordinates(lat.toString(), lng.toString(), country);
    Assert.assertNull(result.getPayload().getLatitude());
    Assert.assertNull(result.getPayload().getLongitude());
    assertEquals(country, result.getPayload().getCountry());
    assertTrue(result.getIssues().contains(OccurrenceIssue.COORDINATES_OUT_OF_RANGE));
  }

  @Test
  public void testLookupLatLngReversed() {
    Double lat = 43.65;
    Double lng = 79.40;
    Country country = Country.CANADA;
    ParseResult<CoordinateCountry> result =
      CoordinateInterpreter.interpretCoordinates(lat.toString(), lng.toString(), country);
    assertEquals(lat, result.getPayload().getLatitude().doubleValue(), 0.0001);
    assertEquals(lng, result.getPayload().getLongitude().doubleValue(), 0.0001);
    assertEquals(country, result.getPayload().getCountry());
    assertTrue(result.getIssues().contains(OccurrenceIssue.PRESUMED_NEGATED_LONGITUDE));
  }

  @Test
  public void testLookupNulls() {
    String lat = null;
    String lng = null;
    Country country = null;
    ParseResult<CoordinateCountry> result = CoordinateInterpreter.interpretCoordinates(lat, lng, country);
    Assert.assertNotNull(result);
    Assert.assertNull(result.getPayload().getCountry());
    Assert.assertNull(result.getPayload().getLatitude());
    Assert.assertNull(result.getPayload().getLongitude());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void testLookupOneNull() {
    String lat = "45";
    String lng = null;
    Country country = null;
    ParseResult<CoordinateCountry> result = CoordinateInterpreter.interpretCoordinates(lat, lng, country);
    Assert.assertNotNull(result);
    Assert.assertNull(result.getPayload().getCountry());
    Assert.assertNull(result.getPayload().getLatitude());
    Assert.assertNull(result.getPayload().getLongitude());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void testRoundPrecision() {
    Double lat = 43.6512345;
    Double lng = -79.4056789;
    // expect max 5 decimals
    double roundedLat = Math.round(lat * Math.pow(10, 5)) / Math.pow(10, 5);
    double roundedLng = Math.round(lng * Math.pow(10, 5)) / Math.pow(10, 5);
    Country country = Country.CANADA;
    ParseResult<CoordinateCountry> result =
      CoordinateInterpreter.interpretCoordinates(lat.toString(), lng.toString(), country);
    assertEquals(roundedLat, result.getPayload().getLatitude(), 0.000001);
    assertEquals(roundedLng, result.getPayload().getLongitude(), 0.000001);
    assertEquals(country, result.getPayload().getCountry());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void testNotNumbers() {
    ParseResult<CoordinateCountry> result = CoordinateInterpreter.interpretCoordinates("asdf", "qwer", null);
    Assert.assertNotNull(result);
  }

  @Test
  public void testFuzzyCountry() {
    // Belfast is UK not IE
    Double lat = 54.597;
    Double lng = -5.93;
    Country country = Country.IRELAND;
    ParseResult<CoordinateCountry> result =
      CoordinateInterpreter.interpretCoordinates(lat.toString(), lng.toString(), country);
    assertEquals(Country.UNITED_KINGDOM, result.getPayload().getCountry());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES));

    // Isle of Man is IM not UK
    lat = 54.25;
    lng = -4.5;
    country = Country.UNITED_KINGDOM;
    result = CoordinateInterpreter.interpretCoordinates(lat.toString(), lng.toString(), country);
    assertEquals(Country.ISLE_OF_MAN, result.getPayload().getCountry());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES));
  }

  @Test
  public void testCountryLoopedupIssue() {
    // Belfast is UK
    Double lat = 54.597;
    Double lng = -5.93;
    ParseResult<CoordinateCountry> result = CoordinateInterpreter.interpretCoordinates(lat.toString(), lng.toString(), null);
    assertEquals(Country.UNITED_KINGDOM, result.getPayload().getCountry());
    assertTrue(result.getIssues().contains(OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES));
  }
}
