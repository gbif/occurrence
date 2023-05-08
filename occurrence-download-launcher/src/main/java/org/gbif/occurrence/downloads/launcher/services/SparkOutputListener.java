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

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.Listener;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.gbif.api.model.occurrence.Download.Status;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

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
