package org.gbif.occurrence.processor.interpreting.util;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.occurrence.processor.interpreting.result.InterpretationResult;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 *
 */
public class BasisOfRecordInterpreterTest {

  @Test
  public void testGoodBor() {
    InterpretationResult<BasisOfRecord> result = BasisOfRecordInterpreter.interpretBasisOfRecord("GermPLASM");
    assertEquals(BasisOfRecord.LIVING_SPECIMEN, result.getPayload());

    result = BasisOfRecordInterpreter.interpretBasisOfRecord("O");
    assertEquals(BasisOfRecord.OBSERVATION, result.getPayload());

    result = BasisOfRecordInterpreter.interpretBasisOfRecord(" specimen");
    assertEquals(BasisOfRecord.PRESERVED_SPECIMEN, result.getPayload());
  }

  @Test
  public void testBadBor() {
    InterpretationResult<BasisOfRecord> result = BasisOfRecordInterpreter.interpretBasisOfRecord("Canada");
    assertNull(result.getPayload());

    result = BasisOfRecordInterpreter.interpretBasisOfRecord(" ");
    assertNull(result.getPayload());

    result = BasisOfRecordInterpreter.interpretBasisOfRecord(null);
    assertNull(result.getPayload());
  }
}
