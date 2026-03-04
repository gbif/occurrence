package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.ObisTerm;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DownloadTermsTest {

  @Test
  public void newEventTermsTest() {
    Assertions.assertTrue(
        DownloadTerms.DOWNLOAD_VERBATIM_TERMS.contains(DwcTerm.fundingAttribution));
    Assertions.assertTrue(
        EventDownloadTerms.DOWNLOAD_VERBATIM_TERMS.contains(DwcTerm.fundingAttribution));
    Assertions.assertFalse(DownloadTerms.DOWNLOAD_VERBATIM_TERMS.contains(GbifTerm.dnaSequenceID));
    Assertions.assertFalse(DownloadTerms.DOWNLOAD_VERBATIM_TERMS.contains(DwcTerm.measurementType));
    Assertions.assertFalse(
        DownloadTerms.DOWNLOAD_VERBATIM_TERMS.contains(ObisTerm.measurementTypeID));
    Assertions.assertFalse(
        EventDownloadTerms.DOWNLOAD_VERBATIM_TERMS.contains(DwcTerm.measurementType));
    Assertions.assertFalse(
        EventDownloadTerms.DOWNLOAD_VERBATIM_TERMS.contains(ObisTerm.measurementTypeID));
  }
}
