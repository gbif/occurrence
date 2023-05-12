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
import org.gbif.common.messaging.api.messages.DownloadCancelMessage;
import org.gbif.occurrence.downloads.launcher.services.DownloadStatusUpdaterService;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher.JobStatus;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/** Listen MQ to receive and run a download */
@Slf4j
@Component
public class DownloadCancellationListener extends AbstractMessageCallback<DownloadCancelMessage> {

  private final DownloadLauncher jobManager;
  private final DownloadStatusUpdaterService downloadStatusUpdaterService;
  private final LockerService lockerService;

  public DownloadCancellationListener(
      DownloadLauncher jobManager,
      DownloadStatusUpdaterService downloadStatusUpdaterService,
      LockerService lockerService) {
    this.jobManager = jobManager;
    this.downloadStatusUpdaterService = downloadStatusUpdaterService;
    this.lockerService = lockerService;
  }

  @Override
  @RabbitListener(queues = "${downloads.cancellationQueueName}")
  public void handleMessage(DownloadCancelMessage downloadsMessage) {
    try {
      log.info("Received message {}", downloadsMessage);
      String downloadId = downloadsMessage.getDownloadId();

      JobStatus jobStatus = jobManager.cancel(downloadId);
      lockerService.unlock(downloadId);
      if (jobStatus == JobStatus.CANCELLED) {
        downloadStatusUpdaterService.updateStatus(downloadId, Status.CANCELLED);
      }
    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      throw new AmqpRejectAndDontRequeueException(ex.getMessage());
    }
  }
}
