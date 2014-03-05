package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.occurrence.processor.interpreting.result.CoordinateCountry;
import org.gbif.occurrence.processor.interpreting.util.CoordinateInterpreter;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Ignore("Requires live geo lookup")
public class CoordinateInterpreterTest {

  @Test
  public void testNull() {
    ParseResult<CoordinateCountry> result = CoordinateInterpreter.interpretCoordinates(null, null, null);
    assertNotNull(result);
    assertEquals(ParseResult.STATUS.FAIL, result.getStatus());

    result = CoordinateInterpreter.interpretCoordinates(null, null, Country.CANADA);
    assertNotNull(result);
    assertEquals(ParseResult.STATUS.FAIL, result.getStatus());

    result = CoordinateInterpreter.interpretCoordinates("10.123", "40.567", null);
    assertNotNull(result);
    assertEquals(ParseResult.STATUS.SUCCESS, result.getStatus());
    assertEquals(10.123, result.getPayload().getLatitude(), 0.0001);
    assertEquals(40.567, result.getPayload().getLongitude(), 0.0001);
  }
}
