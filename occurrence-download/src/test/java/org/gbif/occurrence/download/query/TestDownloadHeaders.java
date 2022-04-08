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

import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.download.hive.DownloadTerms;

import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This test makes sure the Terms used for headers in the download are available in the HDFS table.
 *
 */
public class TestDownloadHeaders {

  @Test
  public void testTermsConsistency(){
    Set<Term> diff = Sets.newHashSet(Sets.symmetricDifference(Sets.newHashSet(TermUtils.interpretedTerms()),
                                                              DownloadTerms.DOWNLOAD_INTERPRETED_TERMS_HDFS));
    diff.remove(GbifTerm.gbifID);
    diff.remove(GbifTerm.verbatimScientificName);
    assertEquals(0, diff.size(),
                 "TermUtils.interpretedTerms() and DownloadTerms.DOWNLOAD_INTERPRETED_TERMS_HDFS must use the same terms. Difference(s): " +
                                 diff);

  }

}
