package org.gbif.occurrence.cli.crawl;

import java.util.List;
import java.util.UUID;

/**
 * Class holding different information about record counts for a dataset.
 */
class DatasetRecordCountInfo {

  private UUID datasetKey;
  private boolean crawlDataConsistent;
  private int lastCompleteCrawlId;
  private long fragmentEmittedCount;
  private long fragmentProcessCount;
  private long solrCount;
  private List<DatasetCrawlInfo> crawlInfo;

  public DatasetRecordCountInfo(){}

  public List<DatasetCrawlInfo> getCrawlInfo() {
    return crawlInfo;
  }

  public void setCrawlInfo(List<DatasetCrawlInfo> crawlInfo) {
    this.crawlInfo = crawlInfo;
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

  public int getLastCompleteCrawlId() {
    return lastCompleteCrawlId;
  }

  public void setLastCompleteCrawlId(int lastCompleteCrawlId) {
    this.lastCompleteCrawlId = lastCompleteCrawlId;
  }

  public long getFragmentEmittedCount() {
    return fragmentEmittedCount;
  }

  public void setFragmentEmittedCount(long fragmentEmittedCount) {
    this.fragmentEmittedCount = fragmentEmittedCount;
  }

  public long getFragmentProcessCount() {
    return fragmentProcessCount;
  }

  public void setFragmentProcessCount(long fragmentProcessCount) {
    this.fragmentProcessCount = fragmentProcessCount;
  }

  public long getSolrCount() {
    return solrCount;
  }

  public void setSolrCount(long solrCount) {
    this.solrCount = solrCount;
  }

  public boolean isCrawlDataConsistent() {
    return crawlDataConsistent;
  }

  public void setCrawlDataConsistent(boolean crawlDataConsistent) {
    this.crawlDataConsistent = crawlDataConsistent;
  }

  public long getDiffSolrLastCrawl() {
    return solrCount - fragmentProcessCount;
  }

  public long getSumAllPreviousCrawl() {
    if(crawlInfo == null || crawlInfo.isEmpty()) {
      return 0;
    }
    return crawlInfo.stream()
            .filter( dci -> dci.getCrawlId() != lastCompleteCrawlId)
            .mapToLong(DatasetCrawlInfo::getCount)
            .sum();
  }

  public double getDiffSolrLastCrawlPercentage() {
    if(solrCount == 0) {
      return 0;
    }
    return (double)getDiffSolrLastCrawl()/(double)solrCount*100d;
  }

  @Override
  public String toString() {
    return "datasetKey: " + datasetKey +
            ", crawlDataConsistent: " + crawlDataConsistent +
            ", lastCompleteCrawlId: " + lastCompleteCrawlId +
            ", fragmentEmittedCount: " + fragmentEmittedCount +
            ", fragmentProcessCount: " + fragmentProcessCount +
            ", solrCount: " + solrCount;
  }

}
