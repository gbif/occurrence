package org.gbif.occurrence.processor.interpreting.util;

import org.gbif.api.vocabulary.Country;
import org.gbif.occurrence.processor.interpreting.result.InterpretationResult;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CountryInterpreterTest {

  @Test
  public void testGoodCountry() {
    InterpretationResult<Country> result = CountryInterpreter.interpretCountry("Canada");
    assertEquals(Country.CANADA, result.getPayload());
  }

  @Test
  public void testBadCaseCountry() {
    InterpretationResult<Country> result = CountryInterpreter.interpretCountry("CanAda");
    assertEquals(Country.CANADA, result.getPayload());
  }

  @Test
  public void testBadCountry() {
    InterpretationResult<Country> result = CountryInterpreter.interpretCountry("Caa");
    assertNull(result.getPayload());
  }

  @Test
  public void testNullCountry() {
    InterpretationResult<Country> result = CountryInterpreter.interpretCountry(null);
    assertNull(result.getPayload());
  }
}
