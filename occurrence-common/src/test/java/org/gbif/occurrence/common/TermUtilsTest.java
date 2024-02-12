/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.common;

import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;

import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

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
    assertTrue(TermUtils.isInterpretedLocalDateSeconds(DwcTerm.dateIdentified));
    assertTrue(TermUtils.isInterpretedUtcDateSeconds(DcTerm.modified));
    assertFalse(TermUtils.isInterpretedLocalDateSeconds(DwcTerm.occurrenceID));
    assertFalse(TermUtils.isInterpretedUtcDateSeconds(DwcTerm.occurrenceID));
    assertTrue(TermUtils.isInterpretedUtcDateMilliseconds(GbifTerm.lastInterpreted));
    // This is an interval, stored as a string and not used for calculations
    assertFalse(TermUtils.isInterpretedLocalDateSeconds(DwcTerm.eventDate));
    assertTrue(TermUtils.isInterpretedLocalDateSeconds(GbifInternalTerm.eventDateGte));
    assertTrue(TermUtils.isInterpretedLocalDateSeconds(GbifInternalTerm.eventDateLte));
  }

  @Test
  public void testIsInterpretedNumerical() throws Exception {
    assertTrue(TermUtils.isInterpretedNumerical(DwcTerm.year));
    assertFalse(TermUtils.isInterpretedNumerical(DwcTerm.occurrenceID));
  }

  @Test
  public void testHiveColumns() {
    assertEquals(GbifTerm.gbifID.simpleName().toLowerCase(), HiveColumnsUtils.getHiveQueryColumn(GbifTerm.gbifID));
    assertEquals(DwcTerm.catalogNumber.simpleName().toLowerCase(),
      HiveColumnsUtils.getHiveQueryColumn(DwcTerm.catalogNumber));
    assertEquals(DcTerm.date.simpleName().toLowerCase() + '_', HiveColumnsUtils.getHiveQueryColumn(DcTerm.date));

    assertEquals(OccurrenceIssue.BASIS_OF_RECORD_INVALID.name().toLowerCase(),
      HiveColumnsUtils.getHiveQueryColumn(OccurrenceIssue.BASIS_OF_RECORD_INVALID));
  }
}
