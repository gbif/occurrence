package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.occurrence.processor.conf.ApiClientConfiguration;
import org.gbif.occurrence.processor.interpreting.result.CoordinateResult;

import java.util.Arrays;

import org.apache.commons.collections.CollectionUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled("Shouldn't be run by Jenkins, it uses an external service")
public class CoordinateInterpreterTest {

  static final ApiClientConfiguration cfg = new ApiClientConfiguration();
  static final CoordinateInterpreter interpreter;
  static {
    cfg.url = "http://api.gbif-dev.org/v1/";
    interpreter = new CoordinateInterpreter(cfg.url);
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
    assertTrue(result.getIssues().contains(OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84));
    assertEquals(2, result.getIssues().size());
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
    assertCountry(43.65, -79.40, Country.CANADA, Country.CANADA);
  }

  @Test
  public void testLookupCountryEmpty() {
    assertCountry(-37.78, 144.97, null, Country.AUSTRALIA, OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES);
  }

  @Test
  public void testLookupCountryMisMatch() {
    // This is Denmark.
    assertCountry(55.68, 12.57, Country.SWEDEN, Country.SWEDEN, OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH);
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
    assertEquals(1, result.getIssues().size());
  }

  @Test
  public void testLookupLatReversed() {
    assertCountry(-43.65, -79.40, Country.CANADA, 43.65, -79.40, Country.CANADA, OccurrenceIssue.PRESUMED_NEGATED_LATITUDE);
  }

  @Test
  public void testLookupLngReversed() {
    assertCountry(43.65, 79.40, Country.CANADA, 43.65, -79.40, Country.CANADA, OccurrenceIssue.PRESUMED_NEGATED_LONGITUDE);
  }

  @Test
  public void testLookupLatLngReversed() {
    assertCountry(-43.65, 79.40, Country.CANADA, 43.65, -79.40, Country.CANADA, OccurrenceIssue.PRESUMED_NEGATED_LATITUDE, OccurrenceIssue.PRESUMED_NEGATED_LONGITUDE);
  }

  @Test
  public void testLookupLatLngSwapped() {
    assertCountry(6.17, 49.75, Country.LUXEMBOURG, 49.75, 6.17, Country.LUXEMBOURG, OccurrenceIssue.PRESUMED_SWAPPED_COORDINATE);
    // With a latitude > 90°, the swap is done by the parser.
    assertCountry(138.86, 36.27, Country.JAPAN, 36.27, 138.86, Country.JAPAN, OccurrenceIssue.PRESUMED_SWAPPED_COORDINATE);
  }

  @Test
  public void testLookupTransformerOrderSwapped() {
    // -0.97 is still in France, and -0.50 is still in DR Congo, but we want the correct order.
    assertCountry(49.43, 0.97, Country.FRANCE, Country.FRANCE);
    assertCountry(0.50, 24.1, Country.CONGO_DEMOCRATIC_REPUBLIC, Country.CONGO_DEMOCRATIC_REPUBLIC);
    // (Unfortunately, this test can still pass by accident if the ordering of the transformers is correct by chance.)
  }

  @Test
  public void testLookupNulls() {
    String lat = null;
    String lng = null;
    Country country = null;
    OccurrenceParseResult<CoordinateResult> result = interpreter.interpretCoordinate(lat, lng, null, country);
    assertNotNull(result);
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
    assertNotNull(result);
    assertNull(result.getPayload());
    assertFalse(result.isSuccessful());
    assertTrue(result.getIssues().isEmpty());
  }

  @Test
  public void testRoundPrecision() {
    Double lat = 43.6512345;
    Double lng = -79.4056789;
    // expect max 6 decimals
    double roundedLat = Math.round(lat * Math.pow(10, 6)) / Math.pow(10, 6);
    double roundedLng = Math.round(lng * Math.pow(10, 6)) / Math.pow(10, 6);
    Country country = Country.CANADA;
    OccurrenceParseResult<CoordinateResult> result =
      interpreter.interpretCoordinate(lat.toString(), lng.toString(), null, country);

    assertCoordinate(result, roundedLat, roundedLng);
    assertEquals(country, result.getPayload().getCountry());

    assertTrue(result.getIssues().contains(OccurrenceIssue.COORDINATE_ROUNDED));
    assertTrue(result.getIssues().contains(OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84));
    assertEquals(2, result.getIssues().size());
  }

  @Test
  public void testNotNumbers() {
    ParseResult<CoordinateResult> result = interpreter.interpretCoordinate("asdf", "qwer", null, null);
    assertNotNull(result);
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
  public void testConfusedCountryWithBadCoordinates() {
    // https://www.gbif.org/occurrence/656979971 — says United States, but coordinates are Northern Mariana Islands
    assertCountry(15.1030, 145.7410, Country.UNITED_STATES, Country.NORTHERN_MARIANA_ISLANDS, OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES);

    // https://www.gbif.org/occurrence/1316597083 — says United States, coordinates are simply wrong, and shouldn't be negated to land on the Northern Mariana Islands.
    assertCountry(-17.35, -149.25, Country.UNITED_STATES, Country.UNITED_STATES, OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH);
  }

  @Test
  public void testAntarctica() {
    // points to Southern Ocean
    assertCountry(-61.21833, 60.02833, Country.ANTARCTICA, Country.ANTARCTICA);
    assertCountry(-61.21833, 60.02833, null, Country.ANTARCTICA, OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES);
  }

  @Test
  public void testLookupLatLngSwappedInAntarctica() {
    assertCountry(-10.0, -85.0, Country.ANTARCTICA, -85.0, -10.0, Country.ANTARCTICA, OccurrenceIssue.PRESUMED_SWAPPED_COORDINATE);
  }

  @Test
  public void testAmbiguousIsoCodeCountries() {
    // Where commonly confused codes exist, accept either and flag an issue.
    // *Except* don't flag an issue if GBIF usage of that ISO code / country differs from the standard.

    // (See the confused-country-pairs.txt file for full details.)

    // Data from Norfolk Island is often labelled Australia
    assertCountry(-29.03, 167.95, Country.AUSTRALIA, Country.NORFOLK_ISLAND, OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES);

    // Data from French Polynesia is sometimes labelled as France
    assertCountry(-17.65, -149.46, Country.FRANCE, Country.FRENCH_POLYNESIA);
    // Data submitted with France but in Réunion will be changed to Réunion.
    assertCountry(-21.1144, 55.5325, Country.FRANCE, Country.RÉUNION);

    // Data from Palestine is sometimes labelled Israel
    assertCountry(32.0, 35.25, Country.ISRAEL, Country.PALESTINIAN_TERRITORY, OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES);

    // Western Sahara / Morocco; don't add an issue.
    assertCountry(27.15, -13.20, Country.WESTERN_SAHARA, Country.MOROCCO);
  }

  @Test
  public void testInTheOcean() {
    assertCountry(0.01, 0.01, null, null);
    // The incorrect country isn't removed, but an issue is added.
    assertCountry(0.01, 0.01, Country.SAO_TOME_PRINCIPE, Country.SAO_TOME_PRINCIPE, OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH);
  }

  private void assertCountry(Double lat, Double lng, Country providedCountry, Country expectedCountry, OccurrenceIssue... expectedIssues) {
    assertCountry(lat, lng, providedCountry, lat, lng, expectedCountry, expectedIssues);
  }

  private void assertCountry(Double lat, Double lng, Country providedCountry, Double expectedLat, Double expectedLng, Country expectedCountry, OccurrenceIssue... expectedIssues) {
    OccurrenceParseResult<CoordinateResult> result = interpreter.interpretCoordinate(lat.toString(), lng.toString(), "EPSG:4326", providedCountry);
    assertCoordinate(result, expectedLat, expectedLng);
    assertEquals(expectedCountry, result.getPayload().getCountry());
    assertTrue(CollectionUtils.isEqualCollection(Arrays.asList(expectedIssues), result.getIssues()), "Expecting "+expectedIssues + " for " + result.getIssues());
    assertEquals(expectedIssues.length, result.getIssues().size());
  }
}
