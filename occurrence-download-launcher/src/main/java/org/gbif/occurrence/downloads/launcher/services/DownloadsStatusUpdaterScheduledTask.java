package org.gbif.occurrence.downloads.launcher.services;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.occurrence.downloads.launcher.services.YarnClientService.Application;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class DownloadsStatusUpdaterScheduledTask {

  private final DownloadStatusUpdaterService statusUpdaterService;
  private final YarnClientService yarnClientService;

  public DownloadsStatusUpdaterScheduledTask(
    DownloadStatusUpdaterService statusUpdaterService, YarnClientService yarnClientService) {
    this.statusUpdaterService = statusUpdaterService;
    this.yarnClientService = yarnClientService;
  }

  @Scheduled(cron = "${downloads.cron}")
  public void updateStatuses() {

    log.info("Running scheduled checker...");
    List<Download> downloads = statusUpdaterService.getExecutingDownloads();

    Set<String> keySet = downloads.stream().map(Download::getKey).collect(Collectors.toSet());
    Map<String, Application> applications = yarnClientService.getAllApplicationByNames(keySet);

    for (Download download : downloads) {
      String downloadKey = download.getKey();
      Application application = applications.get(downloadKey);
      if (application == null) {
        download.setStatus(Status.FAILED);
        statusUpdaterService.updateDownload(download);
      } else if (application.isFinished()) {

        Status status = Status.FAILED;
        YarnApplicationState state = application.getState();

        if (state.equals(YarnApplicationState.FINISHED)) {
          status = Status.SUCCEEDED;
        } else if (state.equals(YarnApplicationState.KILLED)) {
          status = Status.CANCELLED;
        }

        download.setStatus(status);
        statusUpdaterService.updateDownload(download);
      } else {
        log.info(
          "Downloads with downloadKey {} has status {}",
          application.getApplicationId(),
          application.getState().toString());
      }
    }
  }
}
