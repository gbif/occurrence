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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Sets;
import java.util.Set;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.junit.jupiter.api.Test;

/**
 *
 */
public class EventTermUtilsTest {

  @Test
  public void testIsInterpretedSourceTerm() throws Exception {
    assertTrue(EventTermUtils.isInterpretedSourceTerm(DwcTerm.country));
    assertTrue(EventTermUtils.isInterpretedSourceTerm(DwcTerm.countryCode));
    assertTrue(EventTermUtils.isInterpretedSourceTerm(DwcTerm.eventDate));
    assertFalse(EventTermUtils.isInterpretedSourceTerm(DwcTerm.occurrenceID));
    assertFalse(EventTermUtils.isInterpretedSourceTerm(DwcTerm.catalogNumber));
  }

  @Test
  public void testInterpretedTerms() throws Exception {
    System.out.println("\n" + "\nINTERPRETED TERMS");
    Set<Term> terms = Sets.newHashSet();
    for (Term t : EventTermUtils.interpretedTerms()) {
      System.out.println(t.toString());
      assertFalse(terms.contains(t), "Interpreted term exists twice: " + t);
      terms.add(t);
    }
  }


  @Test
  public void testVerbatimTerms() throws Exception {
    System.out.println("\n\nVERBATIM TERMS");
    Set<Term> terms = Sets.newHashSet();
    for (Term t : EventTermUtils.verbatimTerms()) {
      System.out.println(t.toString());
      assertFalse(terms.contains(t), "Verbatim term exists twice: " + t);
      terms.add(t);
    }
  }
}
