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

import org.gbif.common.messaging.api.messages.DownloadLauncherMessage;
import org.gbif.occurrence.downloads.launcher.services.EventDownloadUpdaterService;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.gbif.occurrence.downloads.launcher.services.OccurrenceDownloadUpdaterService;
import org.gbif.occurrence.downloads.launcher.services.launcher.EventDownloadLauncherService;
import org.gbif.occurrence.downloads.launcher.services.launcher.OccurrenceDownloadLauncherService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/** Listen MQ to receive and run a download */
@Component
public class DownloadLauncherListener extends AbstractDownloadLauncherListener {

  public DownloadLauncherListener(
      OccurrenceDownloadLauncherService occurrenceDownloadLauncherService,
      EventDownloadLauncherService eventDownloadLauncherService,
      OccurrenceDownloadUpdaterService occurrenceDownloadUpdaterService,
      EventDownloadUpdaterService eventDownloadUpdaterService,
      LockerService lockerService) {
    super(
        occurrenceDownloadLauncherService,
        eventDownloadLauncherService,
        occurrenceDownloadUpdaterService,
        eventDownloadUpdaterService,
        lockerService);
  }

  @Override
  @RabbitListener(queues = "${downloads.launcherQueueName}")
  public void handleMessage(DownloadLauncherMessage downloadsMessage) {
    super.handleMessage(downloadsMessage);
  }
}
