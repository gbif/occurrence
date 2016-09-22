package org.gbif.occurrence.common.interpretation;

import org.gbif.api.vocabulary.OccurrenceIssue;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 *
 */
public class InterpretationRemarksDefinitionTest {

  /**
   * This test simple makes sure we can load the structure
   */
  @Test
  public void testInterpretationRemarksDefinition(){
    assertFalse(InterpretationRemarksDefinition.REMARKS_MAP.isEmpty());
    assertFalse(InterpretationRemarksDefinition.REMARKS.isEmpty());

    assertNotNull(InterpretationRemarksDefinition.getRelatedTerms(OccurrenceIssue.BASIS_OF_RECORD_INVALID));
    assertNull(InterpretationRemarksDefinition.getRelatedTerms(OccurrenceIssue.CONTINENT_INVALID));
  }
}
