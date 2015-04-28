package org.gbif.occurrence.download.service.conf;

import java.util.Iterator;
import javax.inject.Inject;

import com.google.common.base.Splitter;
import com.google.inject.name.Named;

public class DownloadLimits {

  /**
   * Amount of downloads that an user can execute simultaneously under certain amount of global downloads.
   */
  public static class  Limit {

    private final int maxUserDownloads;

    private final int globalExecutingDownloads;

    public Limit(int maxUserDownloads, int globalExecutingDownloads){
      this.globalExecutingDownloads =globalExecutingDownloads;
      this.maxUserDownloads = maxUserDownloads;
    }

    public int getMaxUserDownloads() {
      return maxUserDownloads;
    }

    public int getGlobalExecutingDownloads() {
      return globalExecutingDownloads;
    }

    public boolean violatesLimit(int userDownloads, int globalDownloads){
      return globalDownloads >= globalExecutingDownloads && userDownloads >= maxUserDownloads;
    }
  }

  private static final Splitter COMMA_SPLITTER = Splitter.on(',');

  private final int maxUserDownloads;

  private final Limit softLimit;
  private final Limit hardLimit;

  public DownloadLimits(int maxUserDownloads, Limit softLimit, Limit hardLimit) {
    this.maxUserDownloads = maxUserDownloads;
    this.softLimit = softLimit;
    this.hardLimit = hardLimit;
  }

  @Inject
  public DownloadLimits(@Named("max_user_downloads") int maxUserDownloads, @Named("downloads_soft_limit") String softLimit,
                        @Named("downloads_hard_limit") String hardLimit) {

    final Iterator<String> softLimits = COMMA_SPLITTER.split(softLimit).iterator();
    final Iterator<String> hardLimits = COMMA_SPLITTER.split(hardLimit).iterator();
    this.maxUserDownloads = maxUserDownloads;
    this.softLimit = new Limit(Integer.parseInt(softLimits.next()),Integer.parseInt(softLimits.next()));
    this.hardLimit = new Limit(Integer.parseInt(hardLimits.next()),Integer.parseInt(hardLimits.next()));
  }

  public int getMaxUserDownloads() {
    return maxUserDownloads;
  }

  public Limit getSoftLimit() {
    return softLimit;
  }

  public Limit getHardLimit() {
    return hardLimit;
  }

  public boolean violatesLimits(int userDownloads, int globalDownloads){
    return getSoftLimit().violatesLimit(userDownloads,globalDownloads) &&
           getHardLimit().violatesLimit(userDownloads,globalDownloads);
  }
}
