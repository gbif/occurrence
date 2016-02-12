package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.processor.guice.ApiClientConfiguration;

import java.net.URI;
import java.util.UUID;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Ignore("Requires live webservices")
public class LocationInterpreterTest {
  static final ApiClientConfiguration cfg = new ApiClientConfiguration();;
  static final LocationInterpreter interpreter;
  static {
    cfg.url = URI.create("http://api.gbif-uat.org/v1/");
    interpreter = new LocationInterpreter(new CoordinateInterpreter(cfg.newApiClient()));
  }

  private VerbatimOccurrence verb;
  private Occurrence occ;

  @Test
  public void testNull() {
    ParseResult result = interpreter.interpretCountry(null);
    assertNotNull(result);
    assertEquals(ParseResult.STATUS.FAIL, result.getStatus());

    result = interpreter.interpretCountry(null, null);
    assertNotNull(result);
    assertEquals(ParseResult.STATUS.FAIL, result.getStatus());
  }

  @Test
  public void testVerbCoordInterp() throws InterruptedException {
    verb = new VerbatimOccurrence();
    verb.setKey(1);
    verb.setDatasetKey(UUID.randomUUID());
    verb.setVerbatimField(DwcTerm.verbatimLatitude, "10.123");
    verb.setVerbatimField(DwcTerm.verbatimLongitude, "55.678");
    occ = new Occurrence(verb);

    interpreter.interpretLocation(verb, occ);
    assertNotNull(occ);
    assertEquals(10.123, occ.getDecimalLatitude(), 0.0001);
    assertEquals(55.678, occ.getDecimalLongitude(), 0.0001);
  }

  @Test
  public void testFullInterp() throws InterruptedException {
    verb = new VerbatimOccurrence();
    verb.setKey(1);
    verb.setDatasetKey(UUID.randomUUID());
    verb.setVerbatimField(DwcTerm.country, "CANADA");
    verb.setVerbatimField(DwcTerm.decimalLatitude, "33.333");
    verb.setVerbatimField(DwcTerm.decimalLongitude, "66.666");
    verb.setVerbatimField(DwcTerm.verbatimLatitude, "10.123");
    verb.setVerbatimField(DwcTerm.verbatimLongitude, "55.678");
    verb.setVerbatimField(DwcTerm.coordinatePrecision, "1.2345");
    occ = new Occurrence(verb);

    interpreter.interpretLocation(verb, occ);
    assertNotNull(occ);
    assertEquals(33.333, occ.getDecimalLatitude(), 0.0001);
    assertEquals(66.666, occ.getDecimalLongitude(), 0.0001);
    assertEquals(1.2345, occ.getCoordinateAccuracy(), 0.0001);
    assertTrue(occ.getIssues().contains(OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH));
    assertTrue(occ.getIssues().contains(OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84));
    assertEquals(2, occ.getIssues().size());
  }

  @Test
  public void testDeriveFromCoordNoCountry() {
    verb = new VerbatimOccurrence();
    verb.setKey(1);
    verb.setDatasetKey(UUID.randomUUID());
    verb.setVerbatimField(DwcTerm.verbatimLatitude, "52.0112");
    verb.setVerbatimField(DwcTerm.verbatimLongitude, "5.2124");
    occ = new Occurrence(verb);

    interpreter.interpretLocation(verb, occ);
    assertNotNull(occ);
    assertEquals(52.0112, occ.getDecimalLatitude(), 0.0001);
    assertEquals(5.2124, occ.getDecimalLongitude(), 0.0001);
    assertEquals(2, occ.getIssues().size());
    assertTrue(occ.getIssues().contains(OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES));
    assertTrue(occ.getIssues().contains(OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84));
    assertEquals(Country.NETHERLANDS, occ.getCountry());
  }

  @Test
  public void testDeriveFromCoordWithCountry() {
    verb = new VerbatimOccurrence();
    verb.setKey(1);
    verb.setDatasetKey(UUID.randomUUID());
    verb.setVerbatimField(DwcTerm.country, "Paraguay");
    verb.setVerbatimField(DwcTerm.verbatimLatitude, "-20.1825");
    verb.setVerbatimField(DwcTerm.verbatimLongitude, "-58.1575");
    occ = new Occurrence(verb);

    interpreter.interpretLocation(verb, occ);
    assertNotNull(occ);
    assertEquals(-20.1825, occ.getDecimalLatitude(), 0.0001);
    assertEquals(-58.1575, occ.getDecimalLongitude(), 0.0001);
    assertEquals(1, occ.getIssues().size());
    assertTrue(occ.getIssues().contains(OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84));
    assertEquals(Country.PARAGUAY, occ.getCountry());
  }

  @Test
  public void testDeriveFromOnlyCountry() {
    verb = new VerbatimOccurrence();
    verb.setKey(1);
    verb.setDatasetKey(UUID.randomUUID());
    verb.setVerbatimField(DwcTerm.country, "Guyana");
    occ = new Occurrence(verb);

    interpreter.interpretLocation(verb, occ);
    assertNotNull(occ);
    assertNull(occ.getDecimalLatitude());
    assertNull(occ.getDecimalLongitude());
    assertEquals(0, occ.getIssues().size());
    assertEquals(Country.GUYANA, occ.getCountry());
  }

  @Test
  public void testDeriveFromOnlyMappedCountry() {
    verb = new VerbatimOccurrence();
    verb.setKey(1);
    verb.setDatasetKey(UUID.randomUUID());
    verb.setVerbatimField(DwcTerm.country, "French Guiana");
    occ = new Occurrence(verb);

    interpreter.interpretLocation(verb, occ);
    assertNotNull(occ);
    assertNull(occ.getDecimalLatitude());
    assertNull(occ.getDecimalLongitude());
    assertEquals(0, occ.getIssues().size());
    assertEquals(Country.FRANCE, occ.getCountry());
  }

  @Test
  public void testAllNulls() throws InterruptedException {
    verb = new VerbatimOccurrence();
    verb.setKey(1);
    verb.setDatasetKey(UUID.randomUUID());
    occ = new Occurrence(verb);

    interpreter.interpretLocation(verb, occ);
    assertNotNull(occ);
  }

  @Test
  public void testDeriveFromEquivalentMappedCountry() throws InterruptedException {
    verb = new VerbatimOccurrence();
    verb.setKey(1);
    verb.setDatasetKey(UUID.randomUUID());
    verb.setVerbatimField(DwcTerm.country, "Martinique");
    verb.setVerbatimField(DwcTerm.decimalLatitude, "14.72");
    verb.setVerbatimField(DwcTerm.decimalLongitude, "-61.06");
    verb.setVerbatimField(DwcTerm.geodeticDatum, "EPSG:4326");
    occ = new Occurrence(verb);

    interpreter.interpretLocation(verb, occ);
    assertNotNull(occ);
    assertEquals(14.72, occ.getDecimalLatitude(), 0.0001);
    assertEquals(-61.06, occ.getDecimalLongitude(), 0.0001);
    assertEquals(Country.FRANCE, occ.getCountry());
    assertEquals(0, occ.getIssues().size());
  }

  @Test
  public void testDeriveFromNotEquivalentMappedCountry() throws InterruptedException {
    verb = new VerbatimOccurrence();
    verb.setKey(1);
    verb.setDatasetKey(UUID.randomUUID());
    verb.setVerbatimField(DwcTerm.country, "France");
    verb.setVerbatimField(DwcTerm.decimalLatitude, "-17.65");
    verb.setVerbatimField(DwcTerm.decimalLongitude, "-149.46");
    verb.setVerbatimField(DwcTerm.geodeticDatum, "EPSG:4326");
    occ = new Occurrence(verb);

    interpreter.interpretLocation(verb, occ);
    assertNotNull(occ);
    assertEquals(-17.65, occ.getDecimalLatitude(), 0.0001);
    assertEquals(-149.46, occ.getDecimalLongitude(), 0.0001);
    assertEquals(Country.FRENCH_POLYNESIA, occ.getCountry());
    assertEquals(1, occ.getIssues().size());
    assertTrue(occ.getIssues().contains(OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES));
  }
}
