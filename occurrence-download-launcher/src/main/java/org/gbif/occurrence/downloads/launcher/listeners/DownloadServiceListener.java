package org.gbif.occurrence.downloads.launcher.listeners;

import java.util.Optional;

import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.occurrence.downloads.launcher.DownloadsMessage;
import org.gbif.occurrence.downloads.launcher.services.DownloadStatusUpdaterService;
import org.gbif.occurrence.downloads.launcher.services.JobManager;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/** Listen MQ to receive and run a download */
@Slf4j
@Component
public class DownloadServiceListener extends AbstractMessageCallback<DownloadsMessage> {

  private final JobManager jobManager;
  private final DownloadStatusUpdaterService downloadStatusUpdaterService;

  public DownloadServiceListener(
    @Qualifier("yarn") JobManager jobManager,
    DownloadStatusUpdaterService downloadStatusUpdaterService) {
    this.jobManager = jobManager;
    this.downloadStatusUpdaterService = downloadStatusUpdaterService;
  }

  @Override
  @RabbitListener(queues = "${downloads.queueName}")
  public void handleMessage(DownloadsMessage downloadsMessage) {
    log.info("Received message {}", downloadsMessage);
    Optional<String> applicationId = jobManager.createJob(downloadsMessage);
    String jobId = downloadsMessage.getJobId();
    if (applicationId.isPresent()) {
      log.info("Running a download for jobId {}, applicationId {}", jobId, applicationId.get());
      downloadStatusUpdaterService.updateStatus(jobId, Status.RUNNING);
    } else {
      log.error("Failed to run a download for jobId {}", downloadsMessage.getJobId());
      downloadStatusUpdaterService.updateStatus(jobId, Status.FAILED);
    }
  }
}
