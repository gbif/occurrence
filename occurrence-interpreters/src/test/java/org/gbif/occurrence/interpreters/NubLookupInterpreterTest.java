package org.gbif.occurrence.interpreters;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.occurrence.interpreters.result.InterpretationResult;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@Ignore("requires live webservice")
public class NubLookupInterpreterTest {

  @Test
  public void testNubLookupGood() {
    InterpretationResult<NameUsageMatch> result =
      NubLookupInterpreter.nubLookup("Animalia", null, null, null, null, "Puma", "Puma concolor", null);
    assertEquals(2435099, result.getPayload().getUsageKey().intValue());
    assertEquals(1, result.getPayload().getKingdomKey().intValue());
    assertEquals("Chordata", result.getPayload().getPhylum());
  }

  @Test
  public void testNubLookupAllNulls() {
    InterpretationResult<NameUsageMatch> result =
      NubLookupInterpreter.nubLookup(null, null, null, null, null, null, null, null);
    assertNotNull(result);
    assertNull(result.getPayload().getScientificName());
  }
}
