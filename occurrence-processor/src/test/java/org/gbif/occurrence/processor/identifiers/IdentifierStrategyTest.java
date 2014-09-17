package org.gbif.occurrence.processor.identifiers;

import org.gbif.api.model.crawler.DwcaValidationReport;
import org.gbif.api.model.crawler.OccurrenceValidationReport;
import org.gbif.api.vocabulary.OccurrenceSchemaType;

import java.util.UUID;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IdentifierStrategyTest {

  @Test
  public void testGoodTripletBadOcc() {
    IdentifierStrategy strategy = new IdentifierStrategy(OccurrenceSchemaType.DWCA,
      new DwcaValidationReport(UUID.randomUUID(), new OccurrenceValidationReport(100, 100, 0, 0, 100, true)));
    assertTrue(strategy.isTripletsValid());
    assertFalse(strategy.isOccurrenceIdsValid());
  }

  @Test
  public void testGoodTripletGoodOcc() {
    IdentifierStrategy strategy = new IdentifierStrategy(OccurrenceSchemaType.DWCA,
      new DwcaValidationReport(UUID.randomUUID(), new OccurrenceValidationReport(100, 100, 0, 100, 0, true)));
    assertTrue(strategy.isTripletsValid());
    assertTrue(strategy.isOccurrenceIdsValid());
  }

  @Test
  public void testDupeTripletGoodOcc() {
    IdentifierStrategy strategy = new IdentifierStrategy(OccurrenceSchemaType.DWCA,
      new DwcaValidationReport(UUID.randomUUID(), new OccurrenceValidationReport(100, 80, 0, 100, 0, true)));
    assertFalse(strategy.isTripletsValid());
    assertTrue(strategy.isOccurrenceIdsValid());
  }

  @Test
  public void testInvalidTripletGoodOcc() {
    IdentifierStrategy strategy = new IdentifierStrategy(OccurrenceSchemaType.DWCA,
      new DwcaValidationReport(UUID.randomUUID(), new OccurrenceValidationReport(100, 70, 30, 100, 0, true)));
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

  @Test
  public void testStrategies() {
    // good triplets, no occ
    int checked = 100;
    int uniqueTriplets = 100;
    int invalidTriplets = 0;
    int uniqueOccIds = 0;
    int missingOccIds = 100;
    DwcaValidationReport report = new DwcaValidationReport(UUID.randomUUID(),
      new OccurrenceValidationReport(checked, uniqueTriplets, invalidTriplets, uniqueOccIds, missingOccIds, true));
    IdentifierStrategy strategy = new IdentifierStrategy(OccurrenceSchemaType.DWCA, report);
    assertTrue(strategy.isTripletsValid());
    assertFalse(strategy.isOccurrenceIdsValid());

    // good triplets, good occ
    checked = 100;
    uniqueTriplets = 100;
    invalidTriplets = 0;
    uniqueOccIds = 100;
    missingOccIds = 0;
    report = new DwcaValidationReport(UUID.randomUUID(),
      new OccurrenceValidationReport(checked, uniqueTriplets, invalidTriplets, uniqueOccIds, missingOccIds, true));
    strategy = new IdentifierStrategy(OccurrenceSchemaType.DWCA, report);
    assertTrue(strategy.isTripletsValid());
    assertTrue(strategy.isOccurrenceIdsValid());

    // dupe triplets, dupe occ
    checked = 100;
    uniqueTriplets = 80;
    invalidTriplets = 0;
    uniqueOccIds = 60;
    missingOccIds = 0;
    report = new DwcaValidationReport(UUID.randomUUID(),
      new OccurrenceValidationReport(checked, uniqueTriplets, invalidTriplets, uniqueOccIds, missingOccIds, true));
    strategy = new IdentifierStrategy(OccurrenceSchemaType.DWCA, report);
    assertFalse(strategy.isTripletsValid());
    assertFalse(strategy.isOccurrenceIdsValid());

    // some invalid triplets but unique matches, some invalid occ, but unique matches
    checked = 100;
    uniqueTriplets = 80;
    invalidTriplets = 20;
    uniqueOccIds = 20;
    missingOccIds = 80;
    report = new DwcaValidationReport(UUID.randomUUID(),
      new OccurrenceValidationReport(checked, uniqueTriplets, invalidTriplets, uniqueOccIds, missingOccIds, true));
    strategy = new IdentifierStrategy(OccurrenceSchemaType.DWCA, report);
    assertTrue(strategy.isTripletsValid());
    assertFalse(strategy.isOccurrenceIdsValid());

    // invalid plus dupe triplets, good occ
    checked = 100;
    uniqueTriplets = 50;
    invalidTriplets = 20;
    uniqueOccIds = 100;
    missingOccIds = 0;
    report = new DwcaValidationReport(UUID.randomUUID(),
      new OccurrenceValidationReport(checked, uniqueTriplets, invalidTriplets, uniqueOccIds, missingOccIds, true));
    strategy = new IdentifierStrategy(OccurrenceSchemaType.DWCA, report);
    assertFalse(strategy.isTripletsValid());
    assertTrue(strategy.isOccurrenceIdsValid());

    // good triplets, invalid and dupe occ
    checked = 100;
    uniqueTriplets = 100;
    invalidTriplets = 0;
    uniqueOccIds = 80;
    missingOccIds = 5;
    report = new DwcaValidationReport(UUID.randomUUID(),
      new OccurrenceValidationReport(checked, uniqueTriplets, invalidTriplets, uniqueOccIds, missingOccIds, true));
    strategy = new IdentifierStrategy(OccurrenceSchemaType.DWCA, report);
    assertTrue(strategy.isTripletsValid());
    assertFalse(strategy.isOccurrenceIdsValid());
  }
}
