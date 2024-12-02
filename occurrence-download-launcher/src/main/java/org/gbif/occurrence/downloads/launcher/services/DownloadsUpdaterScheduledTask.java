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

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher;

import java.util.List;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * Scheduled task is used to update the statuses of the running downloads and unlock those that have
 * updated their statuses.
 */
@Slf4j
@Component
public class DownloadsUpdaterScheduledTask {

  private final DownloadUpdaterService downloadUpdaterService;
  private final DownloadLauncher jobManager;
  private final LockerService lockerService;

  public DownloadsUpdaterScheduledTask(
      DownloadLauncher jobManager,
      DownloadUpdaterService downloadUpdaterService,
      LockerService lockerService) {
    this.jobManager = jobManager;
    this.downloadUpdaterService = downloadUpdaterService;
    this.lockerService = lockerService;
  }

  @Scheduled(cron = "${downloads.taskCron}")
  public void renewedDownloadsStatuses() {
    log.info("Running scheduled check");
    // Get list only of RUNNING or SUSPENDED jobs, because PREPARING can be in the queue
    List<Download> downloads = downloadUpdaterService.getExecutingDownloads();

    log.info("Found {} running downloads", downloads.size());
    if (!downloads.isEmpty()) {
      List<Download> renewedDownloads = jobManager.renewRunningDownloadsStatuses(downloads);
      renewedDownloads.forEach(
          download -> {
            downloadUpdaterService.updateDownload(download);
            if (Status.FINISH_STATUSES.contains(download.getStatus())) {
              lockerService.unlock(download.getKey());
            }
          });
    }

    // Print all locked downloads
    lockerService.printLocks();

    log.info("Finihsed scheduled check");
  }
}
