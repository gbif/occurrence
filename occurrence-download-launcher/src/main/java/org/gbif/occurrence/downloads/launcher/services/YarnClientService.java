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

import static org.apache.hadoop.yarn.api.records.YarnApplicationState.ACCEPTED;
import static org.apache.hadoop.yarn.api.records.YarnApplicationState.FAILED;
import static org.apache.hadoop.yarn.api.records.YarnApplicationState.FINISHED;
import static org.apache.hadoop.yarn.api.records.YarnApplicationState.KILLED;
import static org.apache.hadoop.yarn.api.records.YarnApplicationState.NEW;
import static org.apache.hadoop.yarn.api.records.YarnApplicationState.NEW_SAVING;
import static org.apache.hadoop.yarn.api.records.YarnApplicationState.RUNNING;
import static org.apache.hadoop.yarn.api.records.YarnApplicationState.SUBMITTED;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.gbif.occurrence.downloads.launcher.config.DownloadServiceConfiguration;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class YarnClientService implements Closeable {

  private static final EnumSet<YarnApplicationState> YARN_RUNNING_APPLICATION_STATES =
      EnumSet.of(NEW, NEW_SAVING, SUBMITTED, ACCEPTED, RUNNING);
  private static final EnumSet<YarnApplicationState> YARN_FINISHED_APPLICATION_STATES =
      EnumSet.of(FAILED, KILLED, FINISHED);
  private static final Set<String> APPLICATION_TYPES = Collections.singleton("SPARK");
  private final YarnClient yarnClient;

  public YarnClientService(DownloadServiceConfiguration configuration) {
    this.yarnClient = createYarnClient(configuration);
  }

  public void killApplicationByName(@NotNull String applicationName) {
    try {
      for (ApplicationId applicationId : getRunningApplicationIdByName(applicationName).values()) {
        log.info("Killing applicationName {}, applicationId: {}", applicationName, applicationId);
        yarnClient.killApplication(applicationId);
      }
    } catch (YarnException | IOException ex) {
      log.error("Exception during the killing the application {}", applicationName, ex);
    }
  }

  public Map<String, ApplicationId> getRunningApplicationIdByName(@NotNull String applicationName) {
    return getRunningApplicationIdByName(Collections.singleton(applicationName));
  }

  public Map<String, ApplicationId> getRunningApplicationIdByName(
      @NotNull Set<String> applicationNames) {
    try {
      return yarnClient.getApplications(APPLICATION_TYPES, YARN_RUNNING_APPLICATION_STATES).stream()
          .filter(ar -> applicationNames.contains(ar.getName()))
          .collect(
              Collectors.toMap(
                  ApplicationReport::getName, ApplicationReport::getApplicationId, (a, b) -> b));
    } catch (YarnException | IOException ex) {
      log.error(
          "Exception during the getting applicationIds for applicationNames {}",
          String.join(",", applicationNames),
          ex);
    }
    return Collections.emptyMap();
  }

  public Map<String, Application> getAllApplicationByNames(@NotNull Set<String> applicationNames) {
    try {
      return yarnClient.getApplications(APPLICATION_TYPES).stream()
          .filter(ar -> applicationNames.contains(ar.getName()))
          .collect(
              Collectors.toMap(
                  ApplicationReport::getName,
                  ar -> new Application(ar.getApplicationId(), ar.getYarnApplicationState()),
                  (a, b) -> b));
    } catch (YarnException | IOException ex) {
      log.error(
          "Exception during the getting applicationIds for applicationNames {}",
          String.join(",", applicationNames),
          ex);
    }
    return Collections.emptyMap();
  }

  public Optional<String> getFinishedApplicationNameById(@NotNull String applicationId) {
    try {
      return yarnClient
          .getApplications(APPLICATION_TYPES, YARN_FINISHED_APPLICATION_STATES)
          .stream()
          .filter(ar -> ar.getApplicationId().toString().equals(applicationId))
          .findFirst()
          .map(ApplicationReport::getName);
    } catch (YarnException | IOException ex) {
      log.error(
          "Exception during the getting applicationName for applicationId {}", applicationId, ex);
    }
    return Optional.empty();
  }

  public YarnClient createYarnClient(DownloadServiceConfiguration configuration) {
    log.info("Creating YARN client");

    Configuration cfg = new Configuration();
    cfg.addResource(new Path(configuration.getPathToYarnSite()));

    YarnConfiguration yarnConfiguration = new YarnConfiguration(cfg);
    YarnClient client = YarnClient.createYarnClient();
    client.init(yarnConfiguration);
    client.start();

    return client;
  }

  @Override
  public void close() {
    if (yarnClient != null) {
      try {
        log.info("Closing YARN client connection");
        yarnClient.close();
      } catch (IOException ex) {
        log.error("Can't close YARN client connection");
      }
    }
  }

  @Data
  static class Application {

    private ApplicationId applicationId;
    private YarnApplicationState state;
    private boolean finished;

    public Application(ApplicationId applicationId, YarnApplicationState state) {
      this.applicationId = applicationId;
      this.state = state;
      this.finished = YARN_FINISHED_APPLICATION_STATES.contains(state);
    }
  }
}
