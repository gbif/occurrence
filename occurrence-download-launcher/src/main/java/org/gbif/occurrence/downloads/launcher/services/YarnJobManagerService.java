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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.spark.launcher.Spark2Launcher;
import org.apache.spark.launcher.SparkLauncher;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.common.messaging.api.messages.DownloadLauncherMessage;
import org.gbif.occurrence.downloads.launcher.config.SparkConfiguration;
import org.gbif.occurrence.downloads.launcher.services.YarnClientService.Application;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Slf4j
@Service("yarn")
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class YarnJobManagerService implements JobManager {

  private final SparkConfiguration sparkConfiguration;
  private final YarnClientService yarnClientService;
  private final SparkOutputListener outputListener;

  public YarnJobManagerService(
      YarnClientService yarnClientService,
      SparkConfiguration sparkConfiguration,
      SparkOutputListener outputListener) {
    this.yarnClientService = yarnClientService;
    this.sparkConfiguration = sparkConfiguration;
    this.outputListener = outputListener;
  }

  @Override
  public JobStatus createJob(@NotNull DownloadLauncherMessage message) {
    try {
      String jobId = message.getJobId();

      SparkLauncher launcher =
          new Spark2Launcher()
              // Spark settings
              .setAppName(jobId)
              .setSparkHome("empty") // Workaround for Spark2Launcher
              .setDeployMode(sparkConfiguration.getDeployMode())
              .setMaster(sparkConfiguration.getMaster())
              .setAppResource(sparkConfiguration.getAppResource())
              .setMainClass(sparkConfiguration.getMainClass())
              .setVerbose(sparkConfiguration.isVerbose());

      sparkConfiguration.getFiles().forEach(launcher::addFile);
      // App settings
      launcher.addAppArgs(message.getJobId());
      // Launch
      launcher.startApplication(outputListener);
      // TODO: CALCULATE SPARK RESOURCES?
      // TODO: PASS ALL DOWNLOADS PARAMS

      return JobStatus.RUNNING;

    } catch (Exception ex) {
      log.error("Oops", ex);
      return JobStatus.FAILED;
    }
  }

  @Override
  public JobStatus cancelJob(@NotNull String jobId) {
    yarnClientService.killApplicationByName(jobId);
    return JobStatus.CANCELLED;
  }

  @Override
  public Optional<Status> getStatusByName(String name) {
    Map<String, Application> map =
        yarnClientService.getAllApplicationByNames(Collections.singleton(name));
    return getStatus(map.get(name));
  }

  @Override
  public List<Download> renewRunningDownloadsStatuses(List<Download> downloads) {

    Set<String> keySet = downloads.stream().map(Download::getKey).collect(Collectors.toSet());
    Map<String, Application> applications = yarnClientService.getAllApplicationByNames(keySet);

    List<Download> result = new ArrayList<>();
    for (Download download : downloads) {
      String downloadKey = download.getKey();
      Application application = applications.get(downloadKey);
      getStatus(application)
          .ifPresent(
              status -> {
                download.setStatus(status);
                result.add(download);
              });
    }
    return result;
  }

  private Optional<Status> getStatus(Application application) {
    Status status = null;
    if (application == null) {
      status = Status.FAILED;
    } else if (application.isFinished()) {
      YarnApplicationState state = application.getState();
      if (state.equals(YarnApplicationState.FINISHED)) {
        status = Status.SUCCEEDED;
      } else if (state.equals(YarnApplicationState.KILLED)) {
        status = Status.CANCELLED;
      } else {
        status = Status.FAILED;
      }
    } else {
      log.info(
          "Downloads with applicationId {} and status {}",
          application.getApplicationId(),
          application.getState().toString());
    }
    return Optional.ofNullable(status);
  }
}
