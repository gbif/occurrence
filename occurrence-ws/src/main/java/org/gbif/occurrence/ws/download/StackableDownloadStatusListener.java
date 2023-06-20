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

import java.util.Map;

import javax.inject.Inject;

import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.springframework.stereotype.Component;

/**
 * Downloads status listener. It simply maps the K8 Phase to a JobStatus before calling the Callback Service.
 */
@Component
public class StackableDownloadStatusListener implements StackableSparkWatcher.EventsListener {

  Map<K8StackableSparkController.Phase, JobStatus> PHASE_STATUS_MAP = new ImmutableMap.Builder<K8StackableSparkController.Phase, JobStatus>()
    .put(K8StackableSparkController.Phase.INITIATING, JobStatus.PREP)
    .put(K8StackableSparkController.Phase.PENDING, JobStatus.PREP)
    .put(K8StackableSparkController.Phase.UNKNOWN, JobStatus.PREP)
    .put(K8StackableSparkController.Phase.EMPTY, JobStatus.PREP)
    .put(K8StackableSparkController.Phase.RUNNING, JobStatus.RUNNING)
    .put(K8StackableSparkController.Phase.FAILED, JobStatus.FAILED)
    .put(K8StackableSparkController.Phase.SUCCEEDED, JobStatus.SUCCEEDED)
    .build();

  private final CallbackService callbackService;

  @Inject
  public StackableDownloadStatusListener(CallbackService callbackService) {
    this.callbackService = callbackService;
  }

  @Override
  public void onEvent(StackableSparkWatcher.EventType eventType,
                      String appName,
                      K8StackableSparkController.Phase phase,
                      Object payload) {
    callbackService.processCallback(appName, PHASE_STATUS_MAP.get(phase).name());
  }
}
