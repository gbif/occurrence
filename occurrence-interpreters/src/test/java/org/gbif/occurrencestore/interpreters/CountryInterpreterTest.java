package org.gbif.occurrencestore.interpreters;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CountryInterpreterTest {

  @Test
  public void testGoodCountry() {
    String country = "Canada";
    String result = CountryInterpreter.interpretCountry(country);
    assertEquals("CA", result);
  }

  @Test
  public void testBadCaseCountry() {
    String country = "CanAda";
    String result = CountryInterpreter.interpretCountry(country);
    assertEquals("CA", result);
  }

  @Test
  public void testBadCountry() {
    String country = "Caa";
    String result = CountryInterpreter.interpretCountry(country);
    assertNull(result);
  }

  @Test
  public void testNullCountry() {
    String country = null;
    String result = CountryInterpreter.interpretCountry(country);
    assertNull(result);
  }
}
