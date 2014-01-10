package org.gbif.occurrence.interpreters;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class DepthInterpreterTest {

  @Test
  public void testGoodDepthNoPrecision() {
    String min = "10";
    String max = "100";
    String precision = "0";
    Integer result = DepthInterpreter.interpretDepth(min, max, precision);
    assertEquals(55l, result.longValue());
  }

  @Test
  public void testGoodDepthWithPrecision() {
    // in current implementation precision isn't used
    String min = "10";
    String max = "100";
    String precision = "150";
    Integer result = DepthInterpreter.interpretDepth(min, max, precision);
    assertEquals(55l, result.longValue());
  }

  @Test
  public void testRoundTo0Depth() {
    String min = "0";
    String max = "1";
    String precision = "0";
    Integer result = DepthInterpreter.interpretDepth(min, max, precision);
    assertEquals(0l, result.longValue());
  }

  @Test
  public void testReversedDepth() {
    String min = "100";
    String max = "10";
    String precision = "0";
    Integer result = DepthInterpreter.interpretDepth(min, max, precision);
    assertEquals(55l, result.longValue());
  }

  @Test
  public void testSomeNulls() {
    Integer result = DepthInterpreter.interpretDepth("200", null, null);
    assertNotNull(result);
    assertEquals(200, result.intValue());

    result = DepthInterpreter.interpretDepth(null, "300", null);
    assertNotNull(result);
    assertEquals(300, result.intValue());
  }

  @Test
  public void testAllNulls() {
    Integer result = DepthInterpreter.interpretDepth(null, null, null);
    assertNull(result);
  }
}
