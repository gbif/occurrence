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
import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.DownloadLauncherMessage;
import org.gbif.occurrence.downloads.launcher.services.DownloadUpdaterService;
import org.gbif.occurrence.downloads.launcher.services.EventDownloadUpdaterService;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.gbif.occurrence.downloads.launcher.services.OccurrenceDownloadUpdaterService;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher.JobStatus;
import org.gbif.occurrence.downloads.launcher.services.launcher.EventDownloadLauncherService;
import org.gbif.occurrence.downloads.launcher.services.launcher.OccurrenceDownloadLauncherService;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/** Listen MQ to receive and run a download */
@Slf4j
@Component
public class DownloadLauncherListener extends AbstractMessageCallback<DownloadLauncherMessage> {

  private final OccurrenceDownloadLauncherService occurrenceDownloadLauncherService;
  private final EventDownloadLauncherService eventDownloadLauncherService;
  private final OccurrenceDownloadUpdaterService occurrenceDownloadUpdaterService;
  private final EventDownloadUpdaterService eventDownloadUpdaterService;
  private final LockerService lockerService;

  public DownloadLauncherListener(
      OccurrenceDownloadLauncherService occurrenceDownloadLauncherService,
      EventDownloadLauncherService eventDownloadLauncherService,
      OccurrenceDownloadUpdaterService occurrenceDownloadUpdaterService,
      EventDownloadUpdaterService eventDownloadUpdaterService,
      LockerService lockerService) {
    this.occurrenceDownloadLauncherService = occurrenceDownloadLauncherService;
    this.eventDownloadLauncherService = eventDownloadLauncherService;
    this.occurrenceDownloadUpdaterService = occurrenceDownloadUpdaterService;
    this.eventDownloadUpdaterService = eventDownloadUpdaterService;
    this.lockerService = lockerService;
  }

  @Override
  @RabbitListener(queues = "${downloads.launcherQueueName}")
  public void handleMessage(DownloadLauncherMessage downloadsMessage) {
    try {
      log.info("Received message {}", downloadsMessage);
      String downloadKey = downloadsMessage.getDownloadKey();

      if (!getDownloadUpdaterService(downloadsMessage).isStatusFinished(downloadKey)) {
        JobStatus jobStatus = getDownloadLauncher(downloadsMessage).createRun(downloadKey);

        if (jobStatus == JobStatus.RUNNING) {
          // Keep status as PREPARING, Airflow will mark it as RUNNING when workflow is executed
          log.info("Locking the thread until downloads job is finished");
          lockerService.lock(downloadKey, Thread.currentThread());
          // Status of the download must be updated only in DownloadResource.airflowCallback
        } else if (jobStatus == JobStatus.FAILED) {
          log.error("Failed to process message: {}", downloadsMessage);
          getDownloadUpdaterService(downloadsMessage).updateStatus(downloadKey, Status.FAILED);
          throw new IllegalStateException("Failed to process message");
        } else if (jobStatus == JobStatus.FINISHED) {
          log.warn(
              "Out of sync. Downloads {} status is not finished, but airflow status is finished",
              downloadsMessage);
          getDownloadUpdaterService(downloadsMessage).updateStatus(downloadKey, Status.SUCCEEDED);
        }

      } else {
        log.warn("Download {} has one of finished statuses, ignore further actions", downloadKey);
      }

    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      throw new AmqpRejectAndDontRequeueException(ex.getMessage());
    }
  }

  private DownloadLauncher getDownloadLauncher(DownloadLauncherMessage downloadLauncherMessage) {
    if (downloadLauncherMessage.getDownloadRequest().getType() == DownloadType.EVENT) {
      return eventDownloadLauncherService;
    } else {
      return occurrenceDownloadLauncherService;
    }
  }

  private DownloadUpdaterService getDownloadUpdaterService(
      DownloadLauncherMessage downloadLauncherMessage) {
    if (downloadLauncherMessage.getDownloadRequest().getType() == DownloadType.EVENT) {
      return eventDownloadUpdaterService;
    } else {
      return occurrenceDownloadUpdaterService;
    }
  }
}
