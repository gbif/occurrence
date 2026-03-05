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
package org.gbif.occurrence.download.query;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Sets;
import java.util.Set;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.terms.utils.EventTermUtils;
import org.gbif.terms.utils.TermUtils;
import org.gbif.occurrence.download.hive.DownloadTerms;
import org.gbif.occurrence.download.hive.EventDownloadTerms;
import org.junit.jupiter.api.Test;

/**
 * This test makes sure the Terms used for headers in the download are available in the HDFS table.
 */
public class TestDownloadHeaders {

  @Test
  public void testTermsConsistency() {
    Set<Term> diff =
        Sets.newHashSet(
            Sets.symmetricDifference(
                Sets.newHashSet(TermUtils.interpretedTerms()),
                DownloadTerms.DOWNLOAD_INTERPRETED_TERMS_HDFS));
    diff.remove(GbifTerm.gbifID);
    diff.remove(GbifTerm.verbatimScientificName);
    assertEquals(
        0,
        diff.size(),
        "TermUtils.interpretedTerms() and DownloadTerms.DOWNLOAD_INTERPRETED_TERMS_HDFS must use the same terms. Difference(s): "
            + diff);

    Set<Term> verbatimDiff =
        Sets.newHashSet(
            Sets.symmetricDifference(
                Sets.newHashSet(TermUtils.verbatimTerms()), DownloadTerms.DOWNLOAD_VERBATIM_TERMS));
    verbatimDiff.remove(GbifTerm.gbifID);
    assertEquals(
        0,
        verbatimDiff.size(),
        "TermUtils.verbatimTerms() and DownloadTerms.DOWNLOAD_VERBATIM_TERMS must use the same terms. Difference(s): "
            + verbatimDiff);
  }

  @Test
  public void testEventTermsConsistency() {
    Set<Term> diff =
        Sets.newHashSet(
            Sets.symmetricDifference(
                Sets.newHashSet(EventTermUtils.interpretedTerms()),
                EventDownloadTerms.DOWNLOAD_INTERPRETED_TERMS_HDFS));
    diff.remove(GbifTerm.gbifID);
    diff.remove(GbifTerm.verbatimScientificName);
    assertEquals(
        0,
        diff.size(),
        "EventTermUtils.interpretedTerms() and EventDownloadTerms.DOWNLOAD_INTERPRETED_TERMS_HDFS must use the same terms. Difference(s): "
            + diff);

    Set<Term> verbatimDiff =
        Sets.newHashSet(
            Sets.symmetricDifference(
                Sets.newHashSet(EventTermUtils.verbatimTerms()),
                EventDownloadTerms.DOWNLOAD_VERBATIM_TERMS));
    verbatimDiff.remove(GbifTerm.gbifID);
    assertEquals(
        0,
        verbatimDiff.size(),
        "EventTermUtils.verbatimTerms() and EventDownloadTerms.DOWNLOAD_VERBATIM_TERMS must use the same terms. Difference(s): "
            + verbatimDiff);
  }
}
