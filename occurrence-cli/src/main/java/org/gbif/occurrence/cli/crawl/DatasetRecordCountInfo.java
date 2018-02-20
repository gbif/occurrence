package org.gbif.occurrence.cli.crawl;

import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.crawler.ProcessState;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Class holding different information about record counts for a dataset.
 */
class DatasetRecordCountInfo {

  private UUID datasetKey;

  private int lastCrawlId;
  private int lastCrawlCount;
  private int recordCount;
  private long lastCrawlFragmentEmittedCount;
  private FinishReason finishReason;
  private ProcessState processStateOccurrence;

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

  public long getLastCrawlFragmentEmittedCount() {
    return lastCrawlFragmentEmittedCount;
  }

  public void setLastCrawlFragmentEmittedCount(long lastCrawlFragmentEmittedCount) {
    this.lastCrawlFragmentEmittedCount = lastCrawlFragmentEmittedCount;
  }

  public void setFinishReason(FinishReason finishReason) {
    this.finishReason = finishReason;
  }

  public FinishReason getFinishReason() {
    return finishReason;
  }

  public void setProcessStateOccurrence(ProcessState processStateOccurrence) {
    this.processStateOccurrence = processStateOccurrence;
  }

  public ProcessState getProcessStateOccurrence() {
    return processStateOccurrence;
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

  public int getRecordCount() {
    return recordCount;
  }

  @Override
  public String toString() {
    return "datasetKey: " + datasetKey +
            ", lastCrawlId: " + lastCrawlId +
            ", lastCrawlCount: " + lastCrawlCount +
            ", recordCount: " + recordCount +
            ", lastCrawlFragmentEmittedCount: " + lastCrawlFragmentEmittedCount +
            ", finishReason: " + finishReason +
            ", processStateOccurrence: " + processStateOccurrence +
            ", crawlInfo: {" + crawlInfo + "}";
  }
}
