package org.gbif.occurrence.common;

import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;

import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class TermUtilsTest {

  @Test
  public void testIsInterpretedSourceTerm() throws Exception {
    assertTrue(TermUtils.isInterpretedSourceTerm(DwcTerm.country));
    assertTrue(TermUtils.isInterpretedSourceTerm(DwcTerm.countryCode));
    assertTrue(TermUtils.isInterpretedSourceTerm(DwcTerm.eventDate));
    assertFalse(TermUtils.isInterpretedSourceTerm(DwcTerm.occurrenceID));
    assertFalse(TermUtils.isInterpretedSourceTerm(DwcTerm.catalogNumber));
  }

  @Test
  public void testInterpretedTerms() throws Exception {
    System.out.println("\n" + "\nINTERPRETED TERMS");
    Set<Term> terms = Sets.newHashSet();
    for (Term t : TermUtils.interpretedTerms()) {
      System.out.println(t.toString());
      assertFalse(terms.contains(t), "Interpreted term exists twice: " + t);
      terms.add(t);
    }
  }


  @Test
  public void testExtensionTerms() throws Exception {
    System.out.println("\n\nEXTENSION TERMS");
    for (Extension e : Extension.values()) {
      Term term = TermFactory.instance().findTerm(e.getRowType());
      System.out.println(term.toString());
      assertTrue(TermUtils.isExtensionTerm(term));
    }
  }

  @Test
  public void testVerbatimTerms() throws Exception {
    System.out.println("\n\nVERBATIM TERMS");
    Set<Term> terms = Sets.newHashSet();
    for (Term t : TermUtils.verbatimTerms()) {
      System.out.println(t.toString());
      assertFalse(terms.contains(t), "Verbatim term exists twice: " + t);
      terms.add(t);
    }
  }

  @Test
  public void testIsInterpretedDate() throws Exception {
    assertTrue(TermUtils.isInterpretedLocalDate(DwcTerm.eventDate));
    assertTrue(TermUtils.isInterpretedUtcDate(DcTerm.modified));
    assertFalse(TermUtils.isInterpretedLocalDate(DwcTerm.occurrenceID));
    assertFalse(TermUtils.isInterpretedUtcDate(DwcTerm.occurrenceID));
  }

  @Test
  public void testIsInterpretedNumerical() throws Exception {
    assertTrue(TermUtils.isInterpretedNumerical(DwcTerm.year));
    assertFalse(TermUtils.isInterpretedNumerical(DwcTerm.occurrenceID));
  }

  @Test
  public void testHiveColumns() {
    assertEquals(GbifTerm.gbifID.simpleName().toLowerCase(), HiveColumnsUtils.getHiveColumn(GbifTerm.gbifID));
    assertEquals(DwcTerm.catalogNumber.simpleName().toLowerCase(),
      HiveColumnsUtils.getHiveColumn(DwcTerm.catalogNumber));
    assertEquals(DcTerm.date.simpleName().toLowerCase() + '_', HiveColumnsUtils.getHiveColumn(DcTerm.date));

    assertEquals(OccurrenceIssue.BASIS_OF_RECORD_INVALID.name().toLowerCase(),
      HiveColumnsUtils.getHiveColumn(OccurrenceIssue.BASIS_OF_RECORD_INVALID));
  }
}
