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
package org.gbif.occurrence.download.file.dwca.archive;

import org.gbif.api.model.common.DOI;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.ConjunctionPredicate;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.vocabulary.License;
import org.gbif.occurrence.query.TitleLookupService;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FastaCitationBuilderTest {

  private static final String INSECTA_KEY = "216";
  private static final String TRACHEOPHYTA_KEY = "7707728";
  private static final String DOI_SUFFIX = "dl.test1";
  // DOI.TEST_PREFIX = "10.21373"
  private static final String EXPECTED_DOI_URL = "https://doi.org/10.21373/" + DOI_SUFFIX;

  private TitleLookupService titleLookupService;
  private Date fixedDate;
  private String expectedDate;

  @BeforeEach
  public void setUp() {
    titleLookupService = mock(TitleLookupService.class);
    when(titleLookupService.getSpeciesName(INSECTA_KEY)).thenReturn("Insecta");
    when(titleLookupService.getSpeciesName(TRACHEOPHYTA_KEY)).thenReturn("Tracheophyta");

    Calendar cal = Calendar.getInstance();
    cal.set(2026, Calendar.MARCH, 16, 0, 0, 0);
    cal.set(Calendar.MILLISECOND, 0);
    fixedDate = cal.getTime();
    expectedDate = "16 March 2026";
  }

  private Download buildDownload(Predicate predicate) {
    PredicateDownloadRequest request = new PredicateDownloadRequest();
    request.setType(DownloadType.OCCURRENCE);
    request.setFormat(DownloadFormat.FASTA_ARCHIVE);
    request.setPredicate(predicate);

    DOI doi = new DOI();
    doi.setPrefix(DOI.TEST_PREFIX);
    doi.setSuffix(DOI_SUFFIX);

    Download download = new Download();
    download.setKey("test-key-1");
    download.setDoi(doi);
    download.setRequest(request);
    download.setCreated(fixedDate);
    download.setLicense(License.CC0_1_0);
    return download;
  }

  @Test
  public void targetGeneOnly() {
    Predicate predicate = new EqualsPredicate<>(
      OccurrenceSearchParameter.NUCLEOTIDE_SEQUENCE_TARGET_GENE, "COI", false);

    String citation = FastaCitationBuilder.buildCitation(buildDownload(predicate), titleLookupService);

    assertEquals(
      "FASTA Archive Download of COI sequences. GBIF.org (" + expectedDate + "). " + EXPECTED_DOI_URL,
      citation);
  }

  @Test
  public void targetGeneTaxonAndGeography() {
    Predicate predicate = new ConjunctionPredicate(List.of(
      new EqualsPredicate<>(OccurrenceSearchParameter.NUCLEOTIDE_SEQUENCE_TARGET_GENE, "COI", false),
      new EqualsPredicate<>(OccurrenceSearchParameter.CLASS_KEY, INSECTA_KEY, false),
      new EqualsPredicate<>(OccurrenceSearchParameter.COUNTRY, "NO", false)
    ));

    String citation = FastaCitationBuilder.buildCitation(buildDownload(predicate), titleLookupService);

    assertEquals(
      "FASTA Archive Download of COI sequences from Insecta in Norway. GBIF.org (" + expectedDate + "). " + EXPECTED_DOI_URL,
      citation);
  }

  @Test
  public void targetGeneAndTaxonOnly() {
    Predicate predicate = new ConjunctionPredicate(List.of(
      new EqualsPredicate<>(OccurrenceSearchParameter.NUCLEOTIDE_SEQUENCE_TARGET_GENE, "rbcL", false),
      new EqualsPredicate<>(OccurrenceSearchParameter.TAXON_KEY, TRACHEOPHYTA_KEY, false)
    ));

    String citation = FastaCitationBuilder.buildCitation(buildDownload(predicate), titleLookupService);

    assertEquals(
      "FASTA Archive Download of rbcL sequences from Tracheophyta. GBIF.org (" + expectedDate + "). " + EXPECTED_DOI_URL,
      citation);
  }

  @Test
  public void noFilters() {
    String citation = FastaCitationBuilder.buildCitation(buildDownload(null), titleLookupService);

    assertEquals(
      "FASTA Archive Download. GBIF.org (" + expectedDate + "). " + EXPECTED_DOI_URL,
      citation);
  }

  @Test
  public void nonPredicateRequestReturnsNull() {
    Download download = new Download();
    download.setRequest(new org.gbif.api.model.occurrence.SqlDownloadRequest());
    download.setCreated(fixedDate);

    assertNull(FastaCitationBuilder.buildCitation(download, titleLookupService));
  }
}