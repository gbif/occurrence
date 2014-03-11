package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;

import java.util.UUID;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Ignore("Requires live webservices")
public class LocationInterpreterTest {

  private VerbatimOccurrence verb;
  private Occurrence occ;

  @Test
  public void testVerbCoordInterp() throws InterruptedException {
    verb = new VerbatimOccurrence();
    verb.setKey(1);
    verb.setDatasetKey(UUID.randomUUID());
    verb.setVerbatimField(DwcTerm.verbatimLatitude, "10.123");
    verb.setVerbatimField(DwcTerm.verbatimLongitude, "55.678");
    occ = new Occurrence(verb);

    LocationInterpreter.interpretLocation(verb, occ);
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

    LocationInterpreter.interpretLocation(verb, occ);
    assertNotNull(occ);
    assertEquals(33.333, occ.getDecimalLatitude(), 0.0001);
    assertEquals(66.666, occ.getDecimalLongitude(), 0.0001);
    assertEquals(1.2345, occ.getCoordinateAccuracy(), 0.0001);
    assertEquals(OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH, occ.getIssues().iterator().next());
  }

  @Test
  public void testAllNulls() throws InterruptedException {
    verb = new VerbatimOccurrence();
    verb.setKey(1);
    verb.setDatasetKey(UUID.randomUUID());
    occ = new Occurrence(verb);

    LocationInterpreter.interpretLocation(verb, occ);
    assertNotNull(occ);
  }
}
