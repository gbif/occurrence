package org.gbif.occurrence.downloads.launcher.services;

import org.gbif.api.model.occurrence.Download.Status;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.Listener;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SparkOutputListener implements Listener {

  private final DownloadStatusUpdaterService downloadStatusUpdaterService;
  private final YarnClientService clientService;
  private final LockerService lockerService;

  public SparkOutputListener(
    DownloadStatusUpdaterService downloadStatusUpdaterService,
    YarnClientService clientService,
    LockerService lockerService) {
    this.downloadStatusUpdaterService = downloadStatusUpdaterService;
    this.clientService = clientService;
    this.lockerService = lockerService;
  }

  private static Status mapToDownloadStatus(State state) {
    switch (state) {
      case LOST:
      case FAILED:
      case UNKNOWN:
        return Status.FAILED;
      case KILLED:
        return Status.CANCELLED;
      case RUNNING:
        return Status.RUNNING;
      case FINISHED:
        return Status.SUCCEEDED;
      case CONNECTED:
      case SUBMITTED:
        return Status.SUSPENDED;
      default:
        return Status.FAILED;
    }
  }

  @Override
  public void stateChanged(SparkAppHandle sparkAppHandle) {
    log.info(
      "stateChanged appId: {} state: {}", sparkAppHandle.getAppId(), sparkAppHandle.getState());
    updateStatus(sparkAppHandle);
  }

  @Override
  public void infoChanged(SparkAppHandle sparkAppHandle) {
    log.info(
      "infoChanged appId: {} state: {}", sparkAppHandle.getAppId(), sparkAppHandle.getState());
    updateStatus(sparkAppHandle);
  }

  private void updateStatus(SparkAppHandle sparkAppHandle) {
    String appId = sparkAppHandle.getAppId();
    State state = sparkAppHandle.getState();
    if (appId != null && state != null && state.isFinal()) {
      Status status = mapToDownloadStatus(state);
      clientService
        .getFinishedApplicationNameById(appId)
        .ifPresent(
          name -> {
            downloadStatusUpdaterService.updateStatus(name, status);
            lockerService.unlock(name);
          });
    }
  }
}
