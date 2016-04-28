package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.vocabulary.Rank;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.occurrence.processor.guice.ApiClientConfiguration;

import java.net.URI;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@Ignore("requires live webservice")
public class TaxonomyInterpreterTest {
  static final ApiClientConfiguration cfg = new ApiClientConfiguration();;
  static final TaxonomyInterpreter interpreter;
  static {
    cfg.url = URI.create("http://api.gbif-uat.org/v1/");
    interpreter = new TaxonomyInterpreter(cfg);
  }

  @Test
  public void testNubLookupGood() {
    ParseResult<NameUsageMatch> result =
      interpreter.match("Animalia", null, null, null, null, "Puma", "Puma concolor", null, null, null, null, Rank.SPECIES);
    assertEquals(2435099, result.getPayload().getUsageKey().intValue());
    assertEquals(1, result.getPayload().getKingdomKey().intValue());
    assertEquals("Chordata", result.getPayload().getPhylum());
  }

  @Test
  public void testNubLookupAllNulls() {
    ParseResult<NameUsageMatch> result =
      interpreter.match(null, null, null, null, null, null, null, null, null, null, null, null);
    assertNotNull(result);
    assertNotNull(result.getPayload());
    assertNull(result.getPayload().getScientificName());
  }

  @Test
  public void testNubLookupEmptyStrings() {
    ParseResult<NameUsageMatch> result = interpreter.match("", "", "", "", "", "", "", "", "", "", "", Rank.UNRANKED);
    assertNotNull(result);
    assertNotNull(result.getPayload());
    assertNull(result.getPayload().getScientificName());
  }
}
