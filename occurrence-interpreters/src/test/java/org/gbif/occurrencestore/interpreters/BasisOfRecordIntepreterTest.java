package org.gbif.occurrencestore.interpreters;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BasisOfRecordIntepreterTest {

  @Test
  public void testBasisOfRecordInterpSuccess() {
    Integer result = BasisOfRecordInterpreter.interpretBasisOfRecord("HumanObservation");
    assertEquals(1, result.intValue());
  }

  @Test
  public void testBasisOfRecordInterpNull() {
    Integer result = BasisOfRecordInterpreter.interpretBasisOfRecord(null);
    assertEquals(0, result.intValue());
  }
}
