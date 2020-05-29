package org.gbif.occurrence.download.hive;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static junit.framework.Assert.assertEquals;


/**
 * Check the Terms replacement for TermUtils is consistent.
 */
public class TermsTest {

  @Test
  public void testVerbatimTermsConsistency(){
    testDifferenceAndOrder(
      Lists.newArrayList(TermUtils.verbatimTerms()),
      Terms.verbatimTerms()
    );
  }

  @Test
  public void testInterpretedTermsConsistency(){
    testDifferenceAndOrder(
      Lists.newArrayList(TermUtils.interpretedTerms()),
      Terms.interpretedTerms()
    );
  }

  private void testDifferenceAndOrder(List<Term> fromTermUtils, List<Term> fromTerms) {
    Set<Term> fromTermUtilsSet = Sets.newHashSet(fromTermUtils);
    Set<Term> fromTermsSet = Sets.newHashSet(fromTerms);

    Set<Term> diff = Sets.symmetricDifference(fromTermUtilsSet, fromTermsSet);
    assertEquals("fromTerms and fromTermUtils must use the same terms. Difference(s): " +
      diff, 0, diff.size());

    int i = 0;
    for (; i < fromTermUtils.size(); i++) {
      assertEquals("Order is different at position "+i, fromTermUtils.get(i), fromTerms.get(i));
    }
  }
}
