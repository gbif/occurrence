package org.gbif.occurrence.download.hive;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;

import java.util.List;
import java.util.Set;

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
    Assertions.assertEquals(0, diff.size(), "fromTerms and fromTermUtils must use the same terms. Difference(s): " +
                                            diff);

    int i = 0;
    for (; i < fromTermUtils.size(); i++) {
      Assertions.assertEquals(fromTermUtils.get(i), fromTerms.get(i), "Order is different at position " + i);
    }
  }
}
