package org.gbif.occurrence.downloads.launcher.listeners;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.occurrence.downloads.launcher.DownloadsMessage;
import org.gbif.occurrence.downloads.launcher.services.JobManager;

/** Listen MQ to recive and run a download */
public class DownloadServiceListener extends AbstractMessageCallback<DownloadsMessage> {

  private final JobManager jobManager;

  public DownloadServiceListener(JobManager jobManager) {
    this.jobManager = jobManager;
  }

  @Override
  public void handleMessage(DownloadsMessage downloadsMessage) {
    jobManager.createJob(downloadsMessage);
  }
}
