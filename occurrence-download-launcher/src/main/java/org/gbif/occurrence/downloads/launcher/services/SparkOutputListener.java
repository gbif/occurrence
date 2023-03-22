package org.gbif.occurrence.downloads.launcher.services;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.Listener;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class SparkOutputListener implements Listener {
  private final DownloadStatusUpdaterService downloadStatusUpdaterService;
  private final YarnClientService clientService;

  public SparkOutputListener(DownloadStatusUpdaterService downloadStatusUpdaterService,
      YarnClientService clientService) {
    this.downloadStatusUpdaterService = downloadStatusUpdaterService;
    this.clientService = clientService;
  }

  @Override
  public void stateChanged(SparkAppHandle sparkAppHandle) {
    log.info("stateChanged appId: {} state: {}", sparkAppHandle.getAppId(), sparkAppHandle.getState());
    updateStatus(sparkAppHandle);
  }

  @Override
  public void infoChanged(SparkAppHandle sparkAppHandle) {
    log.info("infoChanged appId: {} state: {}", sparkAppHandle.getAppId(), sparkAppHandle.getState());
    updateStatus(sparkAppHandle);
  }

  private void updateStatus(SparkAppHandle sparkAppHandle){
    String appId = sparkAppHandle.getAppId();
    State state = sparkAppHandle.getState();
    if(appId != null && state != null && state.isFinal()) {
      clientService.getFinishedApplicationNameById(appId)
          .ifPresent(name-> downloadStatusUpdaterService.updateStatus(name, state.name()));
    }
  }
}
