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

import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.DownloadLauncherMessage;
import org.gbif.occurrence.downloads.launcher.services.DownloadUpdaterService;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher.JobStatus;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/** Listen MQ to receive and run a download */
@Slf4j
@Component
public class DownloadLauncherListener extends AbstractMessageCallback<DownloadLauncherMessage> {

  private final DownloadLauncher jobManager;
  private final DownloadUpdaterService downloadUpdaterService;
  private final LockerService lockerService;

  public DownloadLauncherListener(
      DownloadLauncher jobManager,
      DownloadUpdaterService downloadUpdaterService,
      LockerService lockerService) {
    this.jobManager = jobManager;
    this.downloadUpdaterService = downloadUpdaterService;
    this.lockerService = lockerService;
  }

  @Override
  @RabbitListener(queues = "${downloads.launcherQueueName}")
  public void handleMessage(DownloadLauncherMessage downloadsMessage) {
    try {
      log.info("Received message {}", downloadsMessage);
      String downloadKey = downloadsMessage.getDownloadKey();
      ignoreFinishedDownload(downloadKey);

      JobStatus jobStatus = jobManager.create(downloadKey);

      if (jobStatus == JobStatus.RUNNING) {
        // Mark downloads as RUNNING
        downloadUpdaterService.updateStatus(downloadKey, Status.RUNNING);

        log.info("Locking the thread until downloads job is finished");
        lockerService.lock(downloadKey, Thread.currentThread());
      }

    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      throw new AmqpRejectAndDontRequeueException(ex.getMessage());
    }
  }

  private void ignoreFinishedDownload(String downloadKey) {
    if (downloadUpdaterService.isStatusFinished(downloadKey)) {
      log.warn("Download {} has one of finished statuses, ignore further actions", downloadKey);
      throw new IllegalStateException(
          "Download has one of finished statuses, ignore further actions");
    }
  }
}
