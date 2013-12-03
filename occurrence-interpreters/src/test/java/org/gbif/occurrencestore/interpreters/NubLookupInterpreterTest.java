package org.gbif.occurrencestore.interpreters;

import org.gbif.occurrencestore.interpreters.result.NubLookupInterpretationResult;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@Ignore("requires live webservice")
public class NubLookupInterpreterTest {

  @Test
  public void testNubLookupGood() {
    NubLookupInterpretationResult result =
      NubLookupInterpreter.nubLookup("Animalia", null, null, null, null, "Puma", "Puma concolor", null);
    assertEquals(2435099, result.getUsageKey().intValue());
    assertEquals(1, result.getKingdomKey().intValue());
    assertEquals("Chordata", result.getPhylum());
  }

  @Test
  public void testNubLookupAllNulls() {
    NubLookupInterpretationResult result =
      NubLookupInterpreter.nubLookup(null, null, null, null, null, null, null, null);
    assertNotNull(result);
    assertNull(result.getScientificName());
  }
}
