package org.gbif.occurrence.processor.interpreting.util;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.LatLng;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An online converter to verify results available at:
 * http://cs2cs.mygeodata.eu/
 */
public class Wgs84ProjectionTest {

  @Test
  public void testParseCRS() {
    assertNotNull(Wgs84Projection.parseCRS("wgs84"));
    assertNotNull(Wgs84Projection.parseCRS("GDA94"));
    assertNotNull(Wgs84Projection.parseCRS("AGD66"));
    assertNotNull(Wgs84Projection.parseCRS("EPSG:6202"));
    assertNotNull(Wgs84Projection.parseCRS("ED50"));
    assertNotNull(Wgs84Projection.parseCRS("WGS66"));
    assertNotNull(Wgs84Projection.parseCRS("EPSG:3857"));
  }

  @Test
  public void testReproject() {
    // EUROPE
    double lat = 52.61267058;
    double lng = -9.05848491;
    assertLatLon(Wgs84Projection.reproject(lat, lng, "wgs84"), lat, lng, false, false);
    assertLatLon(Wgs84Projection.reproject(lat, lng, null), lat, lng, true, false);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "unknown"), lat, lng, true, false);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "WGS84"), lat, lng, false, false);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "wgs84"), lat, lng, false, false);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "wgs 84"), lat, lng, false, false);
    // this is another form of specifying wgs8ng
    assertLatLon(Wgs84Projection.reproject(lat, lng, "EPSG:4326"), lat, lng, false, false);

    // real projections used frequently in GBIF
    assertLatLon(Wgs84Projection.reproject(lat, lng, "WGS1984"), lat, lng, false, false);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "WGS 84"), lat, lng, false, false);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "World Geodetic System 1984"), lat, lng, false, false);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "WGS_1984"), lat, lng, false, false);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "D_WGS_1984"), lat, lng, false, false);

    // AUSTRALIA
    lat = -34.0;
    lng = 138.0;
    assertLatLon(Wgs84Projection.reproject(lat, lng, "GDA94"), lat, lng, false, false);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "AGD66"), lat+0.001356, lng+0.001041, false, true);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "AGD84"), lat+0.001275, lng+0.001094, false, true);

    // NORTH AMERICA
    lat = 40.0;
    lng = -73.0;
    assertLatLon(Wgs84Projection.reproject(lat, lng, "NAD27"), lat-0.00002, lng+0.000534, false, true);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "North American Datum 1927"), lat-0.00002, lng+0.000534, false, true);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "NAD83"), lat, lng, false, false);

    // OTHER
    // based on http://www.colorado.edu/geography/gcraft/notes/datum/gif/shift.gif
    lat = 30.2;
    lng = -97.7;
    assertLatLon(Wgs84Projection.reproject(lat, lng, "TOKYO"), lat+0.008063, lng-0.002217, false, true);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "ARC 1950"), lat-0.005546, lng-0.001274, false, true);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "CAPE"), lat-0.005581, lng-0.0012493, false, true);
    assertLatLon(Wgs84Projection.reproject(lat, lng, "WGS66"), lat-0.00000427, lng, false, true);
  }

  @Test
  public void testSuspiciousCausesFailure() {
    double lat = 30.2;
    double lng = -97.7;
    // something is wrong with 3857 so we don't write the suspicious transform
    assertFailed(Wgs84Projection.reproject(lat, lng, "EPSG:3857"), lat, lng, OccurrenceIssue.COORDINATE_REPROJECTION_SUSPICIOUS);
  }

  @Test
  public void testFailedParsing() {
    double lat = 43.0;
    double lng = 79.0;
    // FAILED PARSING returns original coords
    assertFailed(Wgs84Projection.reproject(lat, lng, "bla bla bla"), lat, lng, OccurrenceIssue.GEODETIC_DATUM_INVALID);
    assertFailed(Wgs84Projection.reproject(lat, lng, "NAD188"), lat, lng, OccurrenceIssue.GEODETIC_DATUM_INVALID);
    assertFailed(Wgs84Projection.reproject(lat, lng, "ORDNANCE SURVEY 1936"), lat, lng, OccurrenceIssue.COORDINATE_REPROJECTION_FAILED);
  }

  /**
   * Make sure the results are the same whether the code specified was a datum or a full CRS.
   */
  @Test
  public void testDatumVsCrs() {
    // tokyo geodetic datum & CRS
    assertSameTrans(6301, 4301);
    // arc 50
    assertSameTrans(6209, 4209);
    // NAD27
    assertSameTrans(6267, 4267);
    // NAD83
    assertSameTrans(6269, 4269);

  }

  private void assertSameTrans(Integer datumCode, Integer crsCode) {
    double lat = 43.0;
    double lng = 79.0;
    // geodetic CRS
    ParseResult<LatLng> res = Wgs84Projection.reproject(lat, lng, "EPSG:"+crsCode);
    // datum only
    assertLatLon(Wgs84Projection.reproject(lat, lng, "EPSG:" + datumCode),
                 res.getPayload().getLat(),
                 res.getPayload().getLng(), false, true);
  }

  private void assertLatLon(OccurrenceParseResult<LatLng> result, double lat, double lng, boolean assumedWgs, boolean reprojected) {
    if (!assumedWgs) {
      assertTrue(result.isSuccessful(), "parsing failed");
    }
    System.out.println(result.getIssues());
    if (reprojected) {
      assertTrue(result.getIssues().contains(OccurrenceIssue.COORDINATE_REPROJECTED), "Expected reprojection issue");
    }
    if (assumedWgs) {
      assertTrue(result.getIssues().contains(OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84), "Assumed WGS84 issue expected");
    }
    System.out.println(result.getPayload().getLat() + " / " + result.getPayload().getLng());
    assertEquals(lat, result.getPayload().getLat().doubleValue(), 0.000001, "Latitude mismatch");
    assertEquals(lng, result.getPayload().getLng().doubleValue(), 0.000001, "Longitude mismatch");
  }

  private void assertFailed(OccurrenceParseResult<LatLng> result, double lat, double lng, OccurrenceIssue issue) {
    System.out.println(result.getIssues());
    assertFalse(result.isSuccessful(), "parsing expected to fail");
    if (issue == null) {
      assertTrue(result.getIssues().isEmpty(), "Issues found");
    } else {
      assertTrue(result.getIssues().contains(issue), "Expected issue " + issue + " missing");
    }
    assertEquals(lat, result.getPayload().getLat().doubleValue(), 0.000001, "Latitude mismatch");
    assertEquals(lng, result.getPayload().getLng().doubleValue(), 0.000001, "Longitude mismatch");
  }
}
