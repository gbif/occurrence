package org.gbif.occurrence.downloads.launcher.services;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.gbif.occurrence.downloads.launcher.services.YarnClientService.Application;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class DownloadsStatusUpdaterScheduledTask {

  private final DownloadStatusUpdaterService statusUpdaterService;
  private final YarnClientService yarnClientService;

  public DownloadsStatusUpdaterScheduledTask(DownloadStatusUpdaterService statusUpdaterService,
      YarnClientService yarnClientService) {
    this.statusUpdaterService = statusUpdaterService;
    this.yarnClientService = yarnClientService;
  }

  @Scheduled(cron = "${downloads.cron}")
  public void updateStatuses() {
    log.info("Running scheduled checker...");
    // TODO: Get jobIds list from API
    Set<String> jobIds = new HashSet<>(Arrays.asList("Test1", "Test2"));
    Map<String, Application> applications = yarnClientService.getAllApplicationByNames(jobIds);
    for (String jobId : jobIds) {
      Application application = applications.get(jobId);
      if(application == null){
        statusUpdaterService.updateStatus(jobId, "FAILED");
      } else if (application.isFinished()){
        statusUpdaterService.updateStatus(jobId, "FINISHED");
      } else {
        log.info("Downloads with jobId {} has status {}", application.getApplicationId(), application.getState().toString());
      }
    }

  }

}
