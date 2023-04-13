package org.gbif.occurrence.downloads.launcher.listeners;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.occurrence.downloads.launcher.DownloadsMessage;
import org.gbif.occurrence.downloads.launcher.services.DownloadStatusUpdaterService;
import org.gbif.occurrence.downloads.launcher.services.JobManager;
import org.gbif.occurrence.downloads.launcher.services.JobManager.JobStatus;
import org.gbif.occurrence.downloads.launcher.services.LockerService;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/** Listen MQ to receive and run a download */
@Slf4j
@Component
public class DownloadServiceListener extends AbstractMessageCallback<DownloadsMessage> {

  private final JobManager jobManager;
  private final DownloadStatusUpdaterService downloadStatusUpdaterService;
  private final LockerService lockerService;

  public DownloadServiceListener(
    JobManager jobManager,
    DownloadStatusUpdaterService downloadStatusUpdaterService,
    LockerService lockerService) {
    this.jobManager = jobManager;
    this.downloadStatusUpdaterService = downloadStatusUpdaterService;
    this.lockerService = lockerService;
  }

  @Override
  @RabbitListener(queues = "${downloads.queueName}")
  public void handleMessage(DownloadsMessage downloadsMessage) {
    log.info("Received message {}", downloadsMessage);

    JobStatus jobStatus = jobManager.createJob(downloadsMessage);

    if (jobStatus == JobStatus.RUNNING) {
      String jobId = downloadsMessage.getJobId();

      log.info("Locking the thread until downloads job is finished");
      lockerService.lock(jobId, Thread.currentThread());

      jobManager
        .getStatusByName(jobId)
        .ifPresent(status -> downloadStatusUpdaterService.updateStatus(jobId, status));
    }

    if (jobStatus == JobStatus.FAILED) {
      log.error("Failed to process message: {}", downloadsMessage.toString());
      throw new AmqpRejectAndDontRequeueException("Failed to process message");
    }
  }
}
