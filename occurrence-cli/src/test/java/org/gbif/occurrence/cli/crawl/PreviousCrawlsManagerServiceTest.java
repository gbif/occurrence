package org.gbif.occurrence.cli.crawl;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * Unit tests related to {@link PreviousCrawlsManagerConfiguration}.
 */
public class PreviousCrawlsManagerServiceTest {

  @Test
  public void testShouldRunAutomaticDeletionFalse() {
    PreviousCrawlsManagerConfiguration config = new PreviousCrawlsManagerConfiguration();
    config.delete = true;
    config.automaticRecordDeletionThreshold = 30;

    PreviousCrawlsManagerService pcms = new PreviousCrawlsManagerService(config, null);
    PreviousCrawlsManagerService.DatasetRecordCountInfo drci = getDatasetRecordCountInfo();
    drci.setSolrCount(100);
    drci.setFragmentProcessCount(100);
    drci.setLastCompleteCrawlId(2);
    assertFalse("No difference between Solr and last crawl", pcms.shouldRunAutomaticDeletion(drci));

    drci.setSolrCount(118);
    drci.setFragmentEmittedCount(100);
    drci.setFragmentProcessCount(100);
    drci.setCrawlInfo(getCrawlInfoList(getCrawlInfo(1, 2), getCrawlInfo(2, 100)));
    drci.setLastCompleteCrawlId(2);
    assertFalse("Difference between Solr and last crawl is NOT equals to the sum of all previous crawls", pcms.shouldRunAutomaticDeletion(drci));

    drci.setSolrCount(150);
    drci.setFragmentEmittedCount(100);
    drci.setFragmentProcessCount(100);
    drci.setCrawlInfo(getCrawlInfoList(getCrawlInfo(1, 50), getCrawlInfo(2, 100)));
    drci.setLastCompleteCrawlId(2);
    assertFalse("No automatic deletion. Percentage of records to remove (33) higher than the configured threshold (30).",
            pcms.shouldRunAutomaticDeletion(drci));
  }

  /**
   * Test the typical case for auto-deletion.
   */
  @Test
  public void testShouldRunAutomaticDeletionTrue() {
    PreviousCrawlsManagerConfiguration config = new PreviousCrawlsManagerConfiguration();
    config.delete = true;

    PreviousCrawlsManagerService pcms = new PreviousCrawlsManagerService(config, null);
    PreviousCrawlsManagerService.DatasetRecordCountInfo drci = getDatasetRecordCountInfo();

    drci.setSolrCount(118);
    drci.setFragmentEmittedCount(100);
    drci.setFragmentProcessCount(100);
    drci.setCrawlInfo(getCrawlInfoList(getCrawlInfo(1, 18), getCrawlInfo(2, 100)));
    drci.setLastCompleteCrawlId(2);
    assertTrue(pcms.shouldRunAutomaticDeletion(drci));
  }

  /**
   * Generates a DatasetRecordCountInfo with a random datasetKey.
   * @return
   */
  private PreviousCrawlsManagerService.DatasetRecordCountInfo getDatasetRecordCountInfo() {
    PreviousCrawlsManagerService.DatasetRecordCountInfo drci = new PreviousCrawlsManagerService.DatasetRecordCountInfo();
    drci.setDatasetKey(UUID.randomUUID());
    drci.setCrawlDataConsistent(true);
    return drci;
  }

  private DatasetCrawlInfo getCrawlInfo(int crawlId, int count) {
    return new DatasetCrawlInfo(crawlId, count);
  }

  private List<DatasetCrawlInfo> getCrawlInfoList(DatasetCrawlInfo ... allCrawlInfo) {
    return Arrays.asList(allCrawlInfo);
  }

}
