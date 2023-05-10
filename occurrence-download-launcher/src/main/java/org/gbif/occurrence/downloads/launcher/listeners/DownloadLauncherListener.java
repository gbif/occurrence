/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.downloads.launcher.listeners;

import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.DownloadLauncherMessage;
import org.gbif.occurrence.downloads.launcher.services.DownloadStatusUpdaterService;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher.JobStatus;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/** Listen MQ to receive and run a download */
@Slf4j
@Component
public class DownloadLauncherListener extends AbstractMessageCallback<DownloadLauncherMessage> {

  private final DownloadLauncher jobManager;
  private final DownloadStatusUpdaterService downloadStatusUpdaterService;
  private final LockerService lockerService;

  public DownloadLauncherListener(
    DownloadLauncher jobManager,
    DownloadStatusUpdaterService downloadStatusUpdaterService,
    LockerService lockerService) {
    this.jobManager = jobManager;
    this.downloadStatusUpdaterService = downloadStatusUpdaterService;
    this.lockerService = lockerService;
  }

  @Override
  @RabbitListener(queues = "${downloads.launcherQueueName}")
  public void handleMessage(DownloadLauncherMessage downloadsMessage) {
    log.info("Received message {}", downloadsMessage);

    JobStatus jobStatus = jobManager.create(downloadsMessage);

    if (jobStatus == JobStatus.RUNNING) {
      String downloadId = downloadsMessage.getDownloadId();

      // Mark downloads as RUNNING
      downloadStatusUpdaterService.updateStatus(downloadId, Status.RUNNING);

      log.info("Locking the thread until downloads job is finished");
      lockerService.lock(downloadId, Thread.currentThread());

      jobManager
        .getStatusByName(downloadId)
        .ifPresent(status -> downloadStatusUpdaterService.updateStatus(downloadId, status));
    }

    if (jobStatus == JobStatus.FAILED) {
      downloadStatusUpdaterService.updateStatus(downloadsMessage.getDownloadId(), Status.FAILED);
      log.error("Failed to process message: {}", downloadsMessage);
      throw new AmqpRejectAndDontRequeueException("Failed to process message");
    }
  }
}