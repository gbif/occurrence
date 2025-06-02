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
import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.DownloadCancelMessage;
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

/** Listener that cancels a download job. */
@Slf4j
@Component
public class DownloadCancellationListener extends AbstractMessageCallback<DownloadCancelMessage> {

  private final OccurrenceDownloadLauncherService occurrenceDownloadLauncherService;
  private final EventDownloadLauncherService eventDownloadLauncherService;
  private final OccurrenceDownloadUpdaterService occurrenceDownloadUpdaterService;
  private final EventDownloadUpdaterService eventDownloadUpdaterService;
  private final LockerService lockerService;

  public DownloadCancellationListener(
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
  @RabbitListener(queues = "${downloads.cancellationQueueName}")
  public void handleMessage(DownloadCancelMessage downloadsMessage) {
    try {
      log.info("Received message {}", downloadsMessage);
      String downloadKey = downloadsMessage.getDownloadKey();

      JobStatus jobStatus = getDownloadLauncher(downloadsMessage).cancelRun(downloadKey);
      lockerService.unlock(downloadKey);
      getDownloadUpdaterService(downloadsMessage).markAsCancelled(downloadKey);

      log.info("Job cancellation status {}", jobStatus);

    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      throw new AmqpRejectAndDontRequeueException(ex.getMessage());
    }
  }

  private DownloadLauncher getDownloadLauncher(DownloadCancelMessage downloadsMessage) {
    if (downloadsMessage.getDownloadType() == DownloadType.EVENT) {
      return eventDownloadLauncherService;
    } else {
      return occurrenceDownloadLauncherService;
    }
  }

  private DownloadUpdaterService getDownloadUpdaterService(DownloadCancelMessage downloadsMessage) {
    if (downloadsMessage.getDownloadType() == DownloadType.EVENT) {
      return eventDownloadUpdaterService;
    } else {
      return occurrenceDownloadUpdaterService;
    }
  }
}
