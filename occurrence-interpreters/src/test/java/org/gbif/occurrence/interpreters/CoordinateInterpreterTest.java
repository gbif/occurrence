package org.gbif.occurrence.interpreters;


import org.gbif.occurrence.interpreters.result.CoordinateInterpretationResult;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@Ignore("requires live webservice")
public class CoordinateInterpreterTest {

  @Test
  public void testLookupCountryMatch() {
    Double lat = 43.65;
    Double lng = -79.40;
    String country = "CA";
    CoordinateInterpretationResult result =
      CoordinateInterpreter.interpretCoordinates(lat.toString(), lng.toString(), country);
    assertEquals(lat, result.getLatitude().doubleValue(), 0.0001);
    assertEquals(lng, result.getLongitude().doubleValue(), 0.0001);
    assertEquals(country, result.getCountryCode());
    assertEquals(0, result.getGeoSpatialIssue().intValue());
  }

  @Test
  public void testLookupCountryEmpty() {
    Double lat = -37.78;
    Double lng = 144.97;
    CoordinateInterpretationResult result =
      CoordinateInterpreter.interpretCoordinates(lat.toString(), lng.toString(), null);
    assertEquals(lat, result.getLatitude().doubleValue(), 0.0001);
    assertEquals(lng, result.getLongitude().doubleValue(), 0.0001);
    assertEquals("AU", result.getCountryCode());
    assertEquals(0, result.getGeoSpatialIssue().intValue());
  }

  @Test
  public void testLookupCountryMisMatch() {
    Double lat = 55.68;
    Double lng = 12.57;
    String country = "SE";
    CoordinateInterpretationResult result =
      CoordinateInterpreter.interpretCoordinates(lat.toString(), lng.toString(), country);
    assertEquals(lat, result.getLatitude().doubleValue(), 0.0001);
    assertEquals(lng, result.getLongitude().doubleValue(), 0.0001);
    assertEquals(country, result.getCountryCode());
    assertEquals(32, result.getGeoSpatialIssue().intValue());
  }

  @Test
  public void testLookupIllegalCoord() {
    Double lat = 200d;
    Double lng = 200d;
    String country = "asdf";
    CoordinateInterpretationResult result =
      CoordinateInterpreter.interpretCoordinates(lat.toString(), lng.toString(), country);
    assertNull(result.getLatitude());
    assertNull(result.getLongitude());
    assertEquals(country, result.getCountryCode());
    assertEquals(16, result.getGeoSpatialIssue().intValue());
  }

  @Test
  public void testLookupLatLngReversed() {
    Double lat = 43.65;
    Double lng = 79.40;
    String country = "CA";
    CoordinateInterpretationResult result =
      CoordinateInterpreter.interpretCoordinates(lat.toString(), lng.toString(), country);
    assertEquals(lat, result.getLatitude().doubleValue(), 0.0001);
    assertEquals(lng, result.getLongitude().doubleValue(), 0.0001);
    assertEquals(country, result.getCountryCode());
    assertEquals(2, result.getGeoSpatialIssue().intValue());
  }

  @Test
  public void testLookupNulls() {
    String lat = null;
    String lng = null;
    String country = null;
    CoordinateInterpretationResult result = CoordinateInterpreter.interpretCoordinates(lat, lng, country);
    assertNotNull(result);
    assertNull(result.getCountryCode());
    assertNull(result.getLatitude());
    assertNull(result.getLongitude());
    assertNull(result.getGeoSpatialIssue());
  }

  @Test
  public void testLookupOneNull() {
    String lat = "45";
    String lng = null;
    String country = null;
    CoordinateInterpretationResult result = CoordinateInterpreter.interpretCoordinates(lat, lng, country);
    assertNotNull(result);
    assertNull(result.getCountryCode());
    assertNull(result.getLatitude());
    assertNull(result.getLongitude());
    assertNull(result.getGeoSpatialIssue());
  }

  @Test
  public void testRoundPrecision() {
    Double lat = 43.6512345;
    Double lng = -79.4056789;
    // expect max 5 decimals
    double roundedLat = Math.round(lat * Math.pow(10, 5)) / Math.pow(10, 5);
    double roundedLng = Math.round(lng * Math.pow(10, 5)) / Math.pow(10, 5);
    String country = "CA";
    CoordinateInterpretationResult result =
      CoordinateInterpreter.interpretCoordinates(lat.toString(), lng.toString(), country);
    assertEquals(roundedLat, result.getLatitude(), 0.000001);
    assertEquals(roundedLng, result.getLongitude(), 0.000001);
    assertEquals(country, result.getCountryCode());
    assertEquals(0, result.getGeoSpatialIssue().intValue());
  }

  @Test
  public void testNotNumbers() {
    CoordinateInterpretationResult result = CoordinateInterpreter.interpretCoordinates("asdf", "qwer", "zxcv");
    assertNotNull(result);
  }

  @Test
  public void testFuzzyCountry() {
    // Belfast is GB not IE
    Double lat = 54.597;
    Double lng = -5.93;
    String country = "IE";
    CoordinateInterpretationResult result =
      CoordinateInterpreter.interpretCoordinates(lat.toString(), lng.toString(), country);
    assertEquals("GB", result.getCountryCode());
    assertEquals(0, result.getGeoSpatialIssue().intValue());

    // Isle of Man is IM not GB
    lat = 54.25;
    lng = -4.5;
    country = "GB";
    result = CoordinateInterpreter.interpretCoordinates(lat.toString(), lng.toString(), country);
    assertEquals("IM", result.getCountryCode());
    assertEquals(0, result.getGeoSpatialIssue().intValue());
  }
}
