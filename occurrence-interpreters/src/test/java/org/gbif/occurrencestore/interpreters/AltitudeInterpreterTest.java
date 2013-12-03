package org.gbif.occurrencestore.interpreters;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AltitudeInterpreterTest {

  @Test
  public void testAltitudeInterpSuccess() {
    Integer result = AltitudeInterpreter.interpretAltitude("0", "1000", "10");
    assertEquals(500l, result.longValue());
  }

  @Test
  public void testAltitudeInterpNull() {
    Integer result = AltitudeInterpreter.interpretAltitude("asdf", "asdf", "asdf");
    assertNull(result);
  }

  @Test
  public void testAltitudeInterpSomeNulls() {
    Integer result = AltitudeInterpreter.interpretAltitude("2000", null, null);
    assertNotNull(result);
    assertEquals(2000l, result.longValue());

    result = AltitudeInterpreter.interpretAltitude(null, "300", null);
    assertNotNull(result);
    assertEquals(300l, result.longValue());
  }

  @Test
  public void testAltitudeInterpAllNulls() {
    Integer result = AltitudeInterpreter.interpretAltitude(null, null, null);
    assertNull(result);
  }

}
