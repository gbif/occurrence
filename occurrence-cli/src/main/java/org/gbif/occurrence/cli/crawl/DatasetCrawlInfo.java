package org.gbif.occurrence.cli.crawl;

/**
 * Simple holder for crawl info.
 */
class DatasetCrawlInfo {

  private int crawlId;
  private int count;

  public DatasetCrawlInfo(){}

  public DatasetCrawlInfo(int crawlId, int count) {
    this.crawlId = crawlId;
    this.count = count;
  }

  public int getCrawlId() {
    return crawlId;
  }

  public void setCrawlId(int crawlId) {
    this.crawlId = crawlId;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }
}
