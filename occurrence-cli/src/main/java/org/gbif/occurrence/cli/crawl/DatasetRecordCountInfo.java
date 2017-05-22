package org.gbif.occurrence.cli.crawl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Class holding different information about record counts for a dataset.
 */
class DatasetRecordCountInfo {

  private UUID datasetKey;
  //private boolean crawlDataConsistent;
  //private int lastCompleteCrawlId;

  private int lastCrawlId;
  private int lastCrawlCount;
  private int recordCount;

  private long lastCrawlFragmentProcessCount;

  private long currentSolrCount;
  private List<DatasetCrawlInfo> crawlInfo = new ArrayList<>();

  public DatasetRecordCountInfo(){}

  public List<DatasetCrawlInfo> getCrawlInfo() {
    return crawlInfo;
  }

  /**
   * Set the {@link DatasetCrawlInfo} list and compute related variables.
   * @param crawlInfo to be copied internally to a new list
   */
  public void setCrawlInfo(List<DatasetCrawlInfo> crawlInfo) {
    this.crawlInfo = crawlInfo == null ? new ArrayList<>() : new ArrayList<>(crawlInfo);
    computeCrawlData(crawlInfo);
  }

  public DatasetRecordCountInfo(UUID datasetKey) {
    this.datasetKey = datasetKey;
  }

  public UUID getDatasetKey() {
    return datasetKey;
  }

  public void setDatasetKey(UUID datasetKey) {
    this.datasetKey = datasetKey;
  }

  public long getCurrentSolrCount() {
    return currentSolrCount;
  }

  public void setCurrentSolrCount(long currentSolrCount) {
    this.currentSolrCount = currentSolrCount;
  }

  public long getLastCrawlFragmentProcessCount() {
    return lastCrawlFragmentProcessCount;
  }

  public void setLastCrawlFragmentProcessCount(long lastCrawlFragmentProcessCount) {
    this.lastCrawlFragmentProcessCount = lastCrawlFragmentProcessCount;
  }

//  public int getLastCompleteCrawlId() {
//    return lastCompleteCrawlId;
//  }
//
//  public void setLastCompleteCrawlId(int lastCompleteCrawlId) {
//    this.lastCompleteCrawlId = lastCompleteCrawlId;
//  }

  public long getDiffSolrLastCrawl() {
    return currentSolrCount - lastCrawlCount;
  }

  private void computeCrawlData(List<DatasetCrawlInfo> crawlInfo) {

    recordCount = crawlInfo.stream()
            .mapToInt(DatasetCrawlInfo::getCount)
            .sum();

    lastCrawlId = crawlInfo.stream()
            .mapToInt(DatasetCrawlInfo::getCrawlId)
            .max().orElse(-1);

    lastCrawlCount = crawlInfo.stream()
            .mapToInt(DatasetCrawlInfo::getCount)
            .max().orElse(0);
  }

  public double getPercentagePreviousCrawls() {
    return 100 * (double)crawlInfo.stream()
            .filter(dci -> dci.getCrawlId() != lastCrawlId)
            .mapToInt(DatasetCrawlInfo::getCount)
            .sum() / (double)recordCount;
  }

//  public double getDiffSolrLastCrawlPercentage() {
//    if (solrCount == 0) {
//      return 0;
//    }
//    return (double) getDiffSolrLastCrawl() / (double) solrCount * 100d;
//  }

  /**
   * Return the highest crawlId
   * @return id of the last (highest) crawl or -1 if no crawl.
   */
  public int getLastCrawlId() {
    return lastCrawlId;
  }

  public int getLastCrawlCount() {
    return lastCrawlCount;
  }

//  public int getDiffLastCrawlPreviousCrawls() {
//    return lastCrawlCount - getSumAllPreviousCrawl();
//  }

  @Override
  public String toString() {
    return "datasetKey: " + datasetKey +
            ", lastCrawlId: " + lastCrawlId +
            ", lastCrawlCount: " + lastCrawlCount +
            ", lastCrawlFragmentProcessCount: " + lastCrawlFragmentProcessCount +
            ", currentSolrCount: " + currentSolrCount;
  }

}
