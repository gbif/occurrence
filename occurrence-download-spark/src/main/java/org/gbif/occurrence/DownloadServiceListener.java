package org.gbif.occurrence;

/** Listen MQ to recive and run a download */
public class DownloadServiceListener {

  private JobManager jobManager;

  public void createJob(DownloadsMessage message) {
    jobManager.createJob(message);
  }
}
