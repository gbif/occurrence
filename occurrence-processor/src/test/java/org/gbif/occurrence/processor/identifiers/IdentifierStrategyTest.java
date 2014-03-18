package org.gbif.occurrence.processor.identifiers;

import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.vocabulary.OccurrenceSchemaType;

import java.util.UUID;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IdentifierStrategyTest {

  @Test
  public void testGoodTripletBadOcc() {
    IdentifierStrategy strategy = new IdentifierStrategy(OccurrenceSchemaType.DWCA, new DwcaValidationReport(UUID.randomUUID(), 100, 100, 0, 0, 100, true));
    assertTrue(strategy.isTripletsValid());
    assertFalse(strategy.isOccurrenceIdsValid());
  }

  @Test
  public void testGoodTripletGoodOcc() {
    IdentifierStrategy strategy = new IdentifierStrategy(OccurrenceSchemaType.DWCA, new DwcaValidationReport(UUID.randomUUID(), 100, 100, 0, 100, 0, true));
    assertTrue(strategy.isTripletsValid());
    assertTrue(strategy.isOccurrenceIdsValid());
  }

  @Test
  public void testDupeTripletGoodOcc() {
    IdentifierStrategy strategy = new IdentifierStrategy(OccurrenceSchemaType.DWCA, new DwcaValidationReport(UUID.randomUUID(), 100, 80, 0, 100, 0, true));
    assertFalse(strategy.isTripletsValid());
    assertTrue(strategy.isOccurrenceIdsValid());
  }

  @Test
  public void testInvalidTripletGoodOcc() {
    IdentifierStrategy strategy = new IdentifierStrategy(OccurrenceSchemaType.DWCA, new DwcaValidationReport(UUID.randomUUID(), 100, 70, 30, 100, 0, true));
    // as long as triplets are unique we leave the threshold of when there are too many invalid triplets to the DwcaValidationReport
    assertTrue(strategy.isTripletsValid());
    assertTrue(strategy.isOccurrenceIdsValid());
  }

  @Test
  public void testNonDwca() {
    IdentifierStrategy strategy = new IdentifierStrategy(OccurrenceSchemaType.ABCD_2_0_6, null);
    assertTrue(strategy.isTripletsValid());
    assertFalse(strategy.isOccurrenceIdsValid());
  }
}
