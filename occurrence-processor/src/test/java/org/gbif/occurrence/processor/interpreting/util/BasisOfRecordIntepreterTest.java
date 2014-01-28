package org.gbif.occurrence.processor.interpreting.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BasisOfRecordIntepreterTest {

  @Test
  public void testBasisOfRecordInterpSuccess() {
    Integer result = BasisOfRecordInterpreter.interpretBasisOfRecord("HumanObservation");
    Assert.assertEquals(1, result.intValue());
  }

  @Test
  public void testBasisOfRecordInterpNull() {
    Integer result = BasisOfRecordInterpreter.interpretBasisOfRecord(null);
    Assert.assertEquals(0, result.intValue());
  }
}
