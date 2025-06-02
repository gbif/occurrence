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
package org.gbif.occurrence.downloads.launcher.services;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.occurrence.downloads.launcher.services.launcher.EventDownloadLauncherService;
import org.gbif.occurrence.downloads.launcher.services.launcher.OccurrenceDownloadLauncherService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Scheduled task is used to update the statuses of the running downloads and unlock those that have
 * updated their statuses.
 */
@Slf4j
@Component
public class DownloadsUpdaterScheduledTask {

  private final OccurrenceDownloadLauncherService occurrenceDownloadLauncherService;
  private final EventDownloadLauncherService eventDownloadLauncherService;
  private final OccurrenceDownloadUpdaterService occurrenceDownloadUpdaterService;
  private final EventDownloadUpdaterService eventDownloadUpdaterService;
  private final LockerService lockerService;

  public DownloadsUpdaterScheduledTask(
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

  @Scheduled(cron = "${downloads.taskCron}")
  public void renewedDownloadsStatuses() {
    log.info("Running scheduled check for occurrence downloads");
    // Get list only of RUNNING or SUSPENDED jobs, because PREPARING can be in the queue
    List<Download> occurrenceDownloads = occurrenceDownloadUpdaterService.getExecutingDownloads();

    log.info("Found {} running occurrence downloads", occurrenceDownloads.size());
    if (!occurrenceDownloads.isEmpty()) {
      List<Download> renewedDownloads =
          occurrenceDownloadLauncherService.renewRunningDownloadsStatuses(occurrenceDownloads);
      renewedDownloads.forEach(
          download -> {
            occurrenceDownloadUpdaterService.updateDownload(download);
            if (Status.FINISH_STATUSES.contains(download.getStatus())) {
              lockerService.unlock(download.getKey());
            }
          });
    }

    log.info("Running scheduled check for event downloads");
    // Get list only of RUNNING or SUSPENDED jobs, because PREPARING can be in the queue
    List<Download> eventDownloads = eventDownloadUpdaterService.getExecutingDownloads();

    log.info("Found {} running event downloads", eventDownloads.size());
    if (!eventDownloads.isEmpty()) {
      List<Download> renewedDownloads =
          eventDownloadLauncherService.renewRunningDownloadsStatuses(eventDownloads);
      renewedDownloads.forEach(
          download -> {
            eventDownloadUpdaterService.updateDownload(download);
            if (Status.FINISH_STATUSES.contains(download.getStatus())) {
              lockerService.unlock(download.getKey());
            }
          });
    }

    // Print all locked downloads
    lockerService.printLocks();

    log.info("Finished scheduled check");
  }
}
