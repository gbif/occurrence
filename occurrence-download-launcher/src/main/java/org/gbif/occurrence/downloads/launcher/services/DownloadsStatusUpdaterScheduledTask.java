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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DownloadsStatusUpdaterScheduledTask {

  private final DownloadStatusUpdaterService downloadStatusUpdaterService;
  private final JobManager jobManager;
  private final LockerService lockerService;

  public DownloadsStatusUpdaterScheduledTask(
      JobManager jobManager,
      DownloadStatusUpdaterService downloadStatusUpdaterService,
      LockerService lockerService) {
    this.jobManager = jobManager;
    this.downloadStatusUpdaterService = downloadStatusUpdaterService;
    this.lockerService = lockerService;
  }

  @Scheduled(cron = "${downloads.taskCron}")
  public void renewedDownloadsStatuses() {
    log.info("Running scheduled checker...");
    List<Download> downloads = downloadStatusUpdaterService.getExecutingDownloads();

    log.info("Found running downloads - {}", downloads.size());

    if (!downloads.isEmpty()) {
      log.info("Found {} running downloads", downloads.size());
      List<Download> renewedDownloads = jobManager.renewRunningDownloadsStatuses(downloads);
      renewedDownloads.forEach(
          download -> {
            downloadStatusUpdaterService.updateDownload(download);
            if (Status.FINISH_STATUSES.contains(download.getStatus())) {
              lockerService.unlock(download.getKey());
            }
          });
    }
  }
}
