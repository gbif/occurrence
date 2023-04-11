package org.gbif.occurrence.downloads.launcher.services;

import java.util.List;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

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
