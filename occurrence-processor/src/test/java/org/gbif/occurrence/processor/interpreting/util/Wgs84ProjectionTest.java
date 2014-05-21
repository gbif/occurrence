package org.gbif.occurrence.processor.interpreting.util;

import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.LatLng;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class Wgs84ProjectionTest {

  @Test
  public void testParseCRS() {
    assertNotNull(Wgs84Projection.parseCRS("wgs84"));
    assertNotNull(Wgs84Projection.parseCRS("GDA94"));
    assertNotNull(Wgs84Projection.parseCRS("AGD66"));
    assertNotNull(Wgs84Projection.parseCRS("EPSG:6202"));
    assertNotNull(Wgs84Projection.parseCRS("ED50"));
  }

  @Test
  public void testReproject() {
    // EUROPE
    double lat = 43.0;
    double lng = 79.0;
    assertLatLon(Wgs84Projection.reproject(lat, lng, "wgs84"), lat, lng);
    assertLatLon(Wgs84Projection.reproject(lat, lng, null), lat, lng);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "WGS84"), lat, lng);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "wgs84"), lat, lng);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "wgs 84"), lat, lng);
    // this is another form of specifying wgs8ng
    assertLatLon(Wgs84Projection.reproject(lat, lng, "EPSG:4326"), lat, lng);

    // real projections used frequently in GBIF
    assertLatLon(Wgs84Projection.reproject(lat, lng, "WGS1984"), lat, lng);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "WGS 84"), lat, lng);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "World Geodetic System 1984"), lat, lng);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "WGS_1984"), lat, lng);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "D_WGS_1984"), lat, lng);

    // AUSTRALIA
    lat = -34.0;
    lng = 138.0;
    assertLatLon(Wgs84Projection.reproject(lat, lng, "GDA94"), lat, lng);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "AGD66"), lat+0.001356, lng+0.001041);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "AGD84"), lat+0.001275, lng+0.001094);

    // NORTH AMERICA
    lat = 40.0;
    lng = -73.0;
    assertLatLon(Wgs84Projection.reproject(lat, lng, "NAD27"), lat-0.00002, lng+0.000534);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "North American Datum 1927"), lat-0.00002, lng+0.000534);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "NAD83"), lat, lng);
  }

  private void assertLatLon(ParseResult<LatLng> result, double lat, double lng) {
    System.out.println(result.getIssues());
    assertTrue("Issues found", result.getIssues().isEmpty());
    assertTrue("parsing failed", result.isSuccessful());
    assertEquals(lat, result.getPayload().getLat().doubleValue(), 0.000001);
    assertEquals(lng, result.getPayload().getLng().doubleValue(), 0.000001);
  }
}
