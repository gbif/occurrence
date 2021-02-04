package org.gbif.occurrence.hive.udf;

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ToLocalISO8601UDFTest {

  @Test
  public void testLocalDateFormatter() {
    ToLocalISO8601UDF function = new ToLocalISO8601UDF();

    assertNull(function.evaluate(new Text()));
    assertNull(function.evaluate(new Text("")));
    assertEquals("2020-04-02T16:48:54", function.evaluate(new Text("1585846134000")).toString());
    assertEquals("2020-04-02T16:48:54.001", function.evaluate(new Text("1585846134001")).toString());
  }

  @Test
  public void testUtcDateFormatter() {
    ToISO8601UDF function = new ToISO8601UDF();

    assertNull(function.evaluate(new Text()));
    assertNull(function.evaluate(new Text("")));
    assertEquals("2020-04-02T16:48:54Z", function.evaluate(new Text("1585846134000")).toString());
    assertEquals("2020-04-02T16:48:54.001Z", function.evaluate(new Text("1585846134001")).toString());
  }
}
