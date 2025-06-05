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
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.DownloadLauncherMessage;
import org.gbif.occurrence.downloads.launcher.services.EventDownloadUpdaterService;
import org.gbif.occurrence.downloads.launcher.services.OccurrenceDownloadUpdaterService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/** Listen MQ for dead letter queue of download launcher */
@Slf4j
@Component
public class DeadLetterDownloadLauncherListener
    extends AbstractMessageCallback<DownloadLauncherMessage> {

  private final OccurrenceDownloadUpdaterService occurrenceDownloadUpdaterService;
  private final EventDownloadUpdaterService eventDownloadUpdaterService;

  public DeadLetterDownloadLauncherListener(
      OccurrenceDownloadUpdaterService occurrenceDownloadUpdaterService,
      EventDownloadUpdaterService eventDownloadUpdaterService) {
    this.occurrenceDownloadUpdaterService = occurrenceDownloadUpdaterService;
    this.eventDownloadUpdaterService = eventDownloadUpdaterService;
  }

  @Override
  @RabbitListener(queues = "${downloads.deadLauncherQueueName}")
  public void handleMessage(DownloadLauncherMessage downloadsMessage) {
    log.info("Received message {}", downloadsMessage);
    String downloadKey = downloadsMessage.getDownloadKey();
    if (downloadsMessage.getDownloadRequest().getType() == DownloadType.EVENT) {
      eventDownloadUpdaterService.updateStatus(downloadKey, Download.Status.FAILED);
    } else {
      occurrenceDownloadUpdaterService.updateStatus(downloadKey, Download.Status.FAILED);
    }
  }
}
