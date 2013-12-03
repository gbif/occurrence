package org.gbif.occurrencestore.interpreters;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TaxonInterpreterTest {

  @Test
  public void testNameParseGood() {
    String result = TaxonInterpreter.parseName("puma concolor");
    assertEquals("Puma concolor", result);
  }

  @Test
  public void testNameParseNull() {
    String result = TaxonInterpreter.parseName(null);
    assertNull(result);
  }

  @Test
  public void testMapKingdomGood() {
    String result = TaxonInterpreter.mapKingdom("animalia");
    assertEquals("Animalia", result);
  }

  @Test
  public void testMapKingdomUnknown() {
    String result = TaxonInterpreter.mapKingdom("puma");
    assertEquals("puma", result);
  }

  @Test
  public void testMapKingdomNull() {
    String result = TaxonInterpreter.mapKingdom(null);
    assertNull(result);
  }

  @Test
  public void testMapPhylumGood() {
    String result = TaxonInterpreter.mapPhylum("chordata");
    assertEquals("Chordata", result);
  }

  @Test
  public void testMapPhylumUnknown() {
    String result = TaxonInterpreter.mapPhylum("puma");
    assertEquals("puma", result);
  }

  @Test
  public void testMapPhylumNull() {
    String result = TaxonInterpreter.mapPhylum(null);
    assertNull(result);
  }

  @Test
  public void testCleanTaxonGood() {
    String result = TaxonInterpreter.cleanTaxon("Puma concolor\"");
    assertEquals("Puma concolor", result);
  }

  @Test
  public void testCleanTaxonBad() {
    String result = TaxonInterpreter.cleanTaxon("\" \"");
    assertNull(result);
  }

  @Test
  public void testCleanAuthorGood() {
    String result = TaxonInterpreter.cleanAuthor("Linneaus, 1771");
    assertEquals("Linneaus, 1771", result);
  }

  @Test
  public void testCleanAuthorBad() {
    String result = TaxonInterpreter.cleanAuthor("\" \"");
    assertNull(result);
  }
}
