package org.gbif.occurrence.common;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;

import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TermUtilsTest {

  @Test
  public void testIsInterpretedSourceTerm() throws Exception {
    assertTrue(TermUtils.isInterpretedSourceTerm(DwcTerm.eventDate));
    assertTrue(TermUtils.isInterpretedSourceTerm(DwcTerm.occurrenceID));
    assertFalse(TermUtils.isInterpretedSourceTerm(DwcTerm.catalogNumber));
  }

  @Test
  public void testInterpretedTerms() throws Exception {
    System.out.println("\n" + "\nINTERPRETED TERMS");
    Set<Term> terms = Sets.newHashSet();
    for (Term t : TermUtils.interpretedTerms()) {
      System.out.println(t.toString());
      assertFalse("Interpreted term exists twice: " + t, terms.contains(t));
      terms.add(t);
    }
  }

  @Test
  public void testVerbatimTerms() throws Exception {
    System.out.println("\n\nVERBATIM TERMS");
    Set<Term> terms = Sets.newHashSet();
    for (Term t : TermUtils.verbatimTerms()) {
      System.out.println(t.toString());
      assertFalse("Verbatim term exists twice: "+t, terms.contains(t));
      terms.add(t);
    }
  }

  @Test
  public void testIsInterpretedDate() throws Exception {
    assertTrue(TermUtils.isInterpretedDate(DwcTerm.eventDate));
    assertFalse(TermUtils.isInterpretedDate(DwcTerm.occurrenceID));
  }

  @Test
  public void testIsInterpretedNumerical() throws Exception {
    assertTrue(TermUtils.isInterpretedNumerical(DwcTerm.year));
    assertFalse(TermUtils.isInterpretedNumerical(DwcTerm.occurrenceID));
  }
}
