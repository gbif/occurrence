package org.gbif.occurrence.download.query;

import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.download.hive.DownloadTerms;
import org.gbif.occurrence.download.hive.Terms;

import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 * This test makes sure the Terms used for headers in the download are available in the HDFS table.
 *
 */
public class TestDownloadHeaders {

  @Test
  public void testTermsConsistency(){
    Set<Term> interpretedFromTermUtils = Sets.newHashSet(TermUtils.interpretedTerms());
    Set<Term> interpretedFromTerms = Sets.newHashSet(Terms.interpretedTerms());

    Set<Term> diff = Sets.symmetricDifference(interpretedFromTermUtils, interpretedFromTerms);
    assertEquals("TermUtils.interpretedTerms() and Terms.interpretedTerms() must use the same terms. Difference(s): " +
                    diff, 0, diff.size());

    Set<Term> hdfsTerms = DownloadTerms.DOWNLOAD_INTERPRETED_TERMS_HDFS;
    diff = Sets.newHashSet(Sets.symmetricDifference(interpretedFromTermUtils, hdfsTerms));
    diff.remove(GbifTerm.gbifID);
    diff.remove(GbifTerm.mediaType);
    diff.remove(GbifTerm.numberOfOccurrences);
    assertEquals("TermUtils.interpretedTerms() and DownloadTerms.DOWNLOAD_INTERPRETED_TERMS_HDFS must use the same terms. Difference(s): " +
            diff, 0, diff.size());

  }

}
