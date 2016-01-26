package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.occurrence.processor.guice.ApiClientConfiguration;
import org.gbif.occurrence.processor.interpreting.result.CoordinateResult;

import java.net.URI;
import java.util.Arrays;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Ignore("Shouldn't be run by Jenkins, it uses an external service")
public class CoordinateInterpreterTest {

  static final ApiClientConfiguration cfg = new ApiClientConfiguration();
  static final CoordinateInterpreter interpreter;
  static {
    cfg.url = URI.create("http://api.gbif-uat.org/v1/");
    interpreter = new CoordinateInterpreter(cfg.newApiClient());
  }

  private void assertCoordinate(ParseResult<CoordinateResult> result, double lat, double lng) {
    assertEquals(lat, result.getPayload().getLatitude().doubleValue(), 0.00001);
    assertEquals(lng, result.getPayload().getLongitude().doubleValue(), 0.00001);
  }

  @Test
  public void testCoordinate() {
    Double lat = 43.0;
    Double lng = 79.0;
    OccurrenceParseResult<CoordinateResult> result =
      interpreter.interpretCoordinate(lat.toString(), lng.toString(), null, null);

    assertCoordinate(result, lat, lng);
    assertTrue(result.getIssues().contains(OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES));
  }

  @Test
  public void testNull() {
    ParseResult<CoordinateResult> result = interpreter.interpretCoordinate(null, null, null, null);
    assertNotNull(result);
    assertEquals(ParseResult.STATUS.FAIL, result.getStatus());

    result = interpreter.interpretCoordinate(null, null, null, Country.CANADA);
    assertNotNull(result);
    assertEquals(ParseResult.STATUS.FAIL, result.getStatus());

    result = interpreter.interpretCoordinate("10.123", "40.567", null, null);
    assertNotNull(result);
    assertEquals(ParseResult.STATUS.SUCCESS, result.getStatus());

    assertCoordinate(result, 10.123, 40.567);
  }

  @Test
  public void testLookupCountryMatch() {
    Double lat = 43.65;
    Double lng = -79.40;
    Country country = Country.CANADA;
    OccurrenceParseResult<CoordinateResult> result =
      interpreter.interpretCoordinate(lat.toString(), lng.toString(), null, country);

    assertCoordinate(result, lat, lng);
    assertEquals(country, result.getPayload().getCountry());
    assertEquals(1, result.getIssues().size());
    assertTrue(result.getIssues().contains(OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84));
  }

  @Test
  public void testLookupCountryEmpty() {
    Double lat = -37.78;
    Double lng = 144.97;
    OccurrenceParseResult<CoordinateResult> result =
      interpreter.interpretCoordinate(lat.toString(), lng.toString(), null, null);
    assertCoordinate(result, lat, lng);
    assertEquals(Country.AUSTRALIA, result.getPayload().getCountry());
    assertTrue(result.getIssues().contains(OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES));
  }

  @Test
  public void testLookupCountryMisMatch() {
    Double lat = 55.68;
    Double lng = 12.57;
    Country country = Country.SWEDEN;
    OccurrenceParseResult<CoordinateResult> result =
      interpreter.interpretCoordinate(lat.toString(), lng.toString(), null, country);
    assertCoordinate(result, lat, lng);
    assertEquals(country, result.getPayload().getCountry());
    assertTrue(result.getIssues().contains(OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH));
  }

  @Test
  public void testLookupIllegalCoord() {
    Double lat = 200d;
    Double lng = 200d;
    Country country = null; // "asdf"
    OccurrenceParseResult<CoordinateResult> result =
      interpreter.interpretCoordinate(lat.toString(), lng.toString(), null, country);
    assertNull(result.getPayload());
    assertFalse(result.isSuccessful());
    assertTrue(result.getIssues().contains(OccurrenceIssue.COORDINATE_OUT_OF_RANGE));
  }

  @Test
  public void testLookupLatReversed() {
    Double lat = -43.65;
    Double lng = -79.40;
    Country country = Country.CANADA;
    OccurrenceParseResult<CoordinateResult> result =
            interpreter.interpretCoordinate(lat.toString(), lng.toString(), null, country);

    assertCoordinate(result, -1 * lat, lng);
    assertEquals(country, result.getPayload().getCountry());
    assertTrue(result.getIssues().contains(OccurrenceIssue.PRESUMED_NEGATED_LATITUDE));
  }

  @Test
  public void testLookupLngReversed() {
    Double lat = 43.65;
    Double lng = 79.40;
    Country country = Country.CANADA;
    OccurrenceParseResult<CoordinateResult> result =
            interpreter.interpretCoordinate(lat.toString(), lng.toString(), null, country);

    assertCoordinate(result, lat, -1 * lng);
    assertEquals(country, result.getPayload().getCountry());
    assertTrue(result.getIssues().contains(OccurrenceIssue.PRESUMED_NEGATED_LONGITUDE));
  }

  @Test
  public void testLookupLatLngReversed() {
    Double lat = -43.65;
    Double lng = 79.40;
    Country country = Country.CANADA;
    OccurrenceParseResult<CoordinateResult> result =
            interpreter.interpretCoordinate(lat.toString(), lng.toString(), null, country);

    assertCoordinate(result, -1 * lat, -1 * lng);
    assertEquals(country, result.getPayload().getCountry());
    assertTrue(result.getIssues().contains(OccurrenceIssue.PRESUMED_NEGATED_LATITUDE));
    assertTrue(result.getIssues().contains(OccurrenceIssue.PRESUMED_NEGATED_LONGITUDE));
  }

  @Test
  public void testLookupLatLngSwapped() {
    Double lat = 6.17;
    Double lng = 49.75;
    Country country = Country.LUXEMBOURG;
    OccurrenceParseResult<CoordinateResult> result =
            interpreter.interpretCoordinate(lat.toString(), lng.toString(), null, country);

    assertCoordinate(result, lng, lat);
    assertEquals(country, result.getPayload().getCountry());
    assertTrue(result.getIssues().contains(OccurrenceIssue.PRESUMED_SWAPPED_COORDINATE));
  }

  @Test
  public void testLookupNulls() {
    String lat = null;
    String lng = null;
    Country country = null;
    OccurrenceParseResult<CoordinateResult> result = interpreter.interpretCoordinate(lat, lng, null, country);
    Assert.assertNotNull(result);
    assertNull(result.getPayload());
    assertFalse(result.isSuccessful());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void testLookupOneNull() {
    String lat = "45";
    String lng = null;
    Country country = null;
    OccurrenceParseResult<CoordinateResult> result = interpreter.interpretCoordinate(lat, lng, null, country);
    Assert.assertNotNull(result);
    assertNull(result.getPayload());
    assertFalse(result.isSuccessful());
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
    OccurrenceParseResult<CoordinateResult> result =
      interpreter.interpretCoordinate(lat.toString(), lng.toString(), null, country);

    assertCoordinate(result, roundedLat, roundedLng);
    assertEquals(country, result.getPayload().getCountry());

    assertEquals(2, result.getIssues().size());
    assertTrue(result.getIssues().contains(OccurrenceIssue.COORDINATE_ROUNDED));
    assertTrue(result.getIssues().contains(OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84));
  }

  @Test
  public void testNotNumbers() {
    ParseResult<CoordinateResult> result = interpreter.interpretCoordinate("asdf", "qwer", null, null);
    Assert.assertNotNull(result);
  }

  @Test
  public void testConfusedCountry() {
    // Belfast is UK not IE
    assertCountry(54.597, -5.93, Country.IRELAND, Country.UNITED_KINGDOM, OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES);

    // Isle of Man is IM not UK
    assertCountry(54.25, -4.5, Country.UNITED_KINGDOM, Country.ISLE_OF_MAN, OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES);

    // Dublin is IE not UK
    assertCountry(53.35, -6.26, Country.UNITED_KINGDOM, Country.IRELAND, OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES);
  }

  @Test
  public void testCountryLoopedupIssue() {
    // Belfast is UK
    assertCountry(54.597, -5.93, null, Country.UNITED_KINGDOM, OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES);
  }

  @Test
  public void testAntarctica() {
    // points to Southern Ocean
    Double lat = -61.21833;
    Double lng = 60.02833;
    Country country = Country.ANTARCTICA;

    OccurrenceParseResult<CoordinateResult> result =
            interpreter.interpretCoordinate(lat.toString(), lng.toString(), null, country);
    // ensure coordinates are not changed
    assertCoordinate(result, -61.21833, 60.02833);

    // try the same point but without the country
    result = interpreter.interpretCoordinate(lat.toString(), lng.toString(), null, null);
    assertEquals(Country.ANTARCTICA, result.getPayload().getCountry());
  }

  @Test
  public void testLookupLatLngSwappedInAntarctica() {
    Double lat = -10.0;
    Double lng = -85.0;
    Country country = Country.ANTARCTICA;
    OccurrenceParseResult<CoordinateResult> result =
            interpreter.interpretCoordinate(lat.toString(), lng.toString(), null, country);
  }

  @Test
  public void testAmbiguousIsoCodeCountries() {
    // Where commonly confused codes exist, accept either and flag an issue.
    // *Except* don't flag an issue if GBIF usage of that ISO code / country differs from the standard.

    // (See the confused-country-pairs.txt file for full details.)

    // Data from Norfolk Island is often labelled Australia
    assertCountry(-29.03, 167.95, Country.AUSTRALIA, Country.NORFOLK_ISLAND, OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES);

    // Data from French Polynesia is sometimes labelled as France
    assertCountry(-17.65, -149.46, Country.FRANCE, Country.FRENCH_POLYNESIA, OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES);
    // We label data from Réunion with the ISO code for France, which is not strictly what the ISO standard says.
    // Data submitted with Réunion will be changed to France without complaint.
    assertCountry(-21.1144, 55.5325, Country.RÉUNION, Country.FRANCE);

    // Data from Palestine is sometimes labelled Israel
    assertCountry(32.0, 35.0, Country.ISRAEL, Country.PALESTINIAN_TERRITORY, OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES);
  }

  private void assertCountry(Double lat, Double lng, Country providedCountry, Country expectedCountry, OccurrenceIssue... expectedIssues) {
    OccurrenceParseResult<CoordinateResult> result = interpreter.interpretCoordinate(lat.toString(), lng.toString(), "EPSG:4326", providedCountry);
    assertCoordinate(result, lat, lng);
    assertEquals(expectedCountry, result.getPayload().getCountry());
    assertTrue("Expecting "+expectedIssues+" for "+result.getIssues(), CollectionUtils.isEqualCollection(Arrays.asList(expectedIssues), result.getIssues()));
    assertEquals(expectedIssues.length, result.getIssues().size());
  }
}
