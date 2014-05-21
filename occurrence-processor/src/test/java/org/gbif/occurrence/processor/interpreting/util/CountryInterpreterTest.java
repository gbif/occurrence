package org.gbif.occurrence.processor.interpreting.util;

import org.gbif.common.parsers.core.ParseResult;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CountryInterpreterTest {

  @Test
  public void testNull() {
    ParseResult result = CountryInterpreter.interpretCountry(null);
    assertNotNull(result);
    assertEquals(ParseResult.STATUS.FAIL, result.getStatus());

    result = CountryInterpreter.interpretCountry(null, null);
    assertNotNull(result);
    assertEquals(ParseResult.STATUS.FAIL, result.getStatus());
  }
}
