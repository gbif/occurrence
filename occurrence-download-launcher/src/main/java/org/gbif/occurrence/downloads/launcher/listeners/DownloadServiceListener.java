package org.gbif.occurrence.downloads.launcher.listeners;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.occurrence.downloads.launcher.DownloadsMessage;
import org.gbif.occurrence.downloads.launcher.services.JobManager;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/** Listen MQ to recive and run a download */

@Component
public class DownloadServiceListener extends AbstractMessageCallback<DownloadsMessage> {

  private final JobManager jobManager;

  public DownloadServiceListener(@Qualifier("yarn") JobManager jobManager) {
    this.jobManager = jobManager;
  }

  @Override
  @RabbitListener(queues = "${downloads.queueName}")
  public void handleMessage(DownloadsMessage downloadsMessage) {
    jobManager.createJob(downloadsMessage);
  }
}
