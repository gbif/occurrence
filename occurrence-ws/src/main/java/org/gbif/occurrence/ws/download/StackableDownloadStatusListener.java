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
package org.gbif.occurrence.ws.download;

import org.gbif.occurrence.download.service.CallbackService;
import org.gbif.occurrence.download.service.JobStatus;
import org.gbif.stackable.K8StackableSparkController;
import org.gbif.stackable.StackableSparkWatcher;
import org.gbif.stackable.StackableSparkWatcher.EventType;

import java.util.Map;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;

import lombok.extern.slf4j.Slf4j;

/**
 * Downloads status listener. It simply maps the K8 Phase to a JobStatus before calling the Callback Service.
 */
@Slf4j
@Component
public class StackableDownloadStatusListener implements StackableSparkWatcher.EventsListener {

  private static final Map<K8StackableSparkController.Phase, JobStatus> PHASE_STATUS_MAP =
      new ImmutableMap.Builder<K8StackableSparkController.Phase, JobStatus>()
          .put(K8StackableSparkController.Phase.FAILED, JobStatus.FAILED)
          .put(K8StackableSparkController.Phase.SUCCEEDED, JobStatus.SUCCEEDED)
          .build();

  private final CallbackService callbackService;
  private final K8StackableSparkController sparkController;
  private final WatcherConfiguration watcherConfiguration;

  @Inject
  public StackableDownloadStatusListener(CallbackService callbackService, K8StackableSparkController sparkController, WatcherConfiguration watcherConfiguration) {
    this.callbackService = callbackService;
    this.watcherConfiguration = watcherConfiguration;
    this.sparkController = sparkController;
  }

  @Override
  public void onEvent(
      StackableSparkWatcher.EventType eventType,
      String appName,
      K8StackableSparkController.Phase phase,
      Object payload) {
    String selector = watcherConfiguration.getNameSelector().replace(".*", "");
    String downloadKey = appName.replace(selector, "");
    try {
      JobStatus jobStatus = PHASE_STATUS_MAP.get(phase);
      if (jobStatus != null && eventType != EventType.DELETED) {
        callbackService.processCallback(downloadKey, jobStatus.name());
        log.info("Stopping K8s Spark job with name {}", appName);
        sparkController.stopSparkApplication(appName);
      }
    } catch (Exception ex) {
      log.error("onEvent K8S issue {}", appName, ex);
    }
  }
}
