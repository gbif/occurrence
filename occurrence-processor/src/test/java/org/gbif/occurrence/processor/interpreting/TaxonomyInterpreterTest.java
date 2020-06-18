package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.vocabulary.Rank;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.occurrence.processor.conf.ApiClientConfiguration;
import org.junit.Ignore;
import org.junit.Test;


import static org.junit.Assert.*;

@Ignore("requires live webservice")
public class TaxonomyInterpreterTest {
  static final ApiClientConfiguration cfg = new ApiClientConfiguration();;
  static final TaxonomyInterpreter interpreter;
  static {
    cfg.url = "http://api.gbif-uat.org/v1/";
    interpreter = new TaxonomyInterpreter(cfg);
  }

  @Test
  public void testAssembledAuthor() {
    ParseResult<NameUsageMatch> result = interpreter.match("Animalia", null, null, null, null, "Puma", "Puma concolor", "", null, null, null, Rank.SPECIES);
    assertEquals(2435099, result.getPayload().getUsageKey().intValue());
    assertEquals(1, result.getPayload().getKingdomKey().intValue());
    assertEquals("Chordata", result.getPayload().getPhylum());

    result = interpreter.match("Animalia", null, null, null, null, "Puma", "Puma concolor (Linnaeus, 1771)", null, null, null, null, Rank.SPECIES);
    assertEquals(2435099, result.getPayload().getUsageKey().intValue());
    assertEquals(1, result.getPayload().getKingdomKey().intValue());
    assertEquals("Chordata", result.getPayload().getPhylum());

    result = interpreter.match("Animalia", null, null, null, null, "Puma", "Puma concolor", "(Linnaeus, 1771)", null, null, null, Rank.SPECIES);
    assertEquals(2435099, result.getPayload().getUsageKey().intValue());
    assertEquals(1, result.getPayload().getKingdomKey().intValue());
    assertEquals("Chordata", result.getPayload().getPhylum());
  }

  @Test
  public void testOenanthe() {
    ParseResult<NameUsageMatch> result = interpreter.match("Plantae", null, null, null, null, null, "Oenanthe", "", null, null, null, Rank.GENUS);
    assertEquals(3034893, result.getPayload().getUsageKey().intValue());
    assertEquals(6, result.getPayload().getKingdomKey().intValue());
    assertEquals("Oenanthe L.", result.getPayload().getScientificName());

    result = interpreter.match("Plantae", null, null, null, null, null, "Oenanthe", "L.", null, null, null, Rank.GENUS);
    assertEquals(3034893, result.getPayload().getUsageKey().intValue());
    assertEquals(6, result.getPayload().getKingdomKey().intValue());
    assertEquals("Oenanthe L.", result.getPayload().getScientificName());

    result = interpreter.match("Animalia", null, null, null, null, null, "Oenanthe", "Vieillot, 1816", null, null, null, Rank.GENUS);
    assertEquals(2492483, result.getPayload().getUsageKey().intValue());
    assertEquals(1, result.getPayload().getKingdomKey().intValue());
    assertEquals("Oenanthe Vieillot, 1816", result.getPayload().getScientificName());
  }

  @Test
  @Ignore
  public void testOtu() {
    ParseResult<NameUsageMatch> result = interpreter.match("Animalia", "Annelida", null, null, "Lumbricidae", null, "BOLD:ACV7160", null, null, null, null, Rank.SPECIES);
    assertEquals("BOLD:ACV7160", result.getPayload().getScientificName());
  }

  @Test
  public void testCeratiaceae() {
    ParseResult<NameUsageMatch> result = interpreter.match("Chromista", "Dinophyta", "Dinophyceae", "Peridiniales", "Ceratiaceae", "Ceratium", "Ceratium hirundinella", "", null, null, null, Rank.SPECIES);
    assertEquals(7598904, result.getPayload().getUsageKey().intValue());
    assertEquals(7479242, result.getPayload().getFamilyKey().intValue());
    assertEquals("Ceratium hirundinella (O.F.Müller) Dujardin, 1841", result.getPayload().getScientificName());
  }

  @Test
  public void testNubLookupGood() {
    ParseResult<NameUsageMatch> result = interpreter.match("Animalia", null, null, null, null, "Puma", "Puma concolor", null, null, null, null, Rank.SPECIES);
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
