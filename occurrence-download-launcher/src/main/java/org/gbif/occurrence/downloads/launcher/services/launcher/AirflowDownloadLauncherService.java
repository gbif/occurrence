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
package org.gbif.occurrence.downloads.launcher.services.launcher;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.occurrence.downloads.launcher.pojo.AirflowConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.SparkStaticConfiguration;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.gbif.occurrence.downloads.launcher.services.launcher.airflow.AirflowBody;
import org.gbif.occurrence.downloads.launcher.services.launcher.airflow.AirflowRunner;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AirflowDownloadLauncherService implements DownloadLauncher {

  private final SparkStaticConfiguration sparkStaticConfiguration;
  private final AirflowRunner airflowRunner;
  private final OccurrenceDownloadClient downloadClient;
  private final AirflowConfiguration airflowConfiguration;
  private final LockerService lockerService;

  public AirflowDownloadLauncherService(
    SparkStaticConfiguration sparkStaticConfiguration,
    AirflowConfiguration airflowConfiguration,
    OccurrenceDownloadClient downloadClient,
    LockerService lockerService) {
    this.sparkStaticConfiguration = sparkStaticConfiguration;
    this.downloadClient = downloadClient;
    this.airflowConfiguration = airflowConfiguration;
    this.airflowRunner = AirflowRunner.builder().airflowConfiguration(airflowConfiguration).build();
    this.lockerService = lockerService;
  }


  private boolean isSmallDownload(Download download) {
    return sparkStaticConfiguration.getSmallDownloadCutOff() >= download.getTotalRecords();
  }

  private int executorInstances(Download download) {
     return isSmallDownload(download)
       ? sparkStaticConfiguration.getMinInstances()
       : Math.min(sparkStaticConfiguration.getMaxInstances(), Math.max((int)download.getTotalRecords() / sparkStaticConfiguration.getRecordsPerInstance(), 1));
  }

  private AirflowBody getAirflowBody(Download download) {

    int driverMemory = sparkStaticConfiguration.getDriverResources().getMemory().getLimitGb() * 1024;
    int driverCpu =Integer.parseInt(sparkStaticConfiguration.getDriverResources().getCpu().getMin().replace("m", ""));
    int executorMemory = sparkStaticConfiguration.getExecutorResources().getMemory().getLimitGb() * 1024;
    int executorCpu = Integer.parseInt(sparkStaticConfiguration.getExecutorResources().getCpu().getMin().replace("m", ""));
    int memoryOverhead = sparkStaticConfiguration.getMemoryOverheadMb();
    //Given as megabytes (Mi)
    int vectorMemory = sparkStaticConfiguration.getVectorMemory();
    // Given as whole CPUs
    int vectorCpu = sparkStaticConfiguration.getVectorCpu();
    // Calculate values for Yunikorn annotation
    // Driver
    int driverMinResourceMemory =  Double.valueOf(Math.ceil((driverMemory + vectorMemory) / 1024d)).intValue();
    int driverMinResourceCpu =  driverCpu + vectorCpu;
    // Executor
    int executorMinResourceMemory =  Double.valueOf(Math.ceil((executorMemory + memoryOverhead + vectorMemory) / 1024d)).intValue();
    int executorMinResourceCpu = executorCpu + vectorCpu;
    return AirflowBody.builder()
      .conf(AirflowBody.Conf.builder()
        .args(Lists.newArrayList(download.getKey(), download.getRequest().getType().getCoreTerm().name(), "/stackable/spark/jobs/download.properties"))
        // Driver
        .driverMinCpu(sparkStaticConfiguration.getDriverResources().getCpu().getMin())
        .driverMaxCpu(sparkStaticConfiguration.getDriverResources().getCpu().getMax())
        .driverLimitMemory(sparkStaticConfiguration.getDriverResources().getMemory().getLimitGb() + "Gi")
        .driverMinResourceMemory(driverMinResourceMemory + "Gi")
        .driverMinResourceCpu(driverMinResourceCpu + "m")
        // Executor
        .memoryOverhead(String.valueOf(sparkStaticConfiguration.getMemoryOverheadMb()))
        .executorMinResourceMemory(executorMinResourceMemory + "Gi")
        .executorMinResourceCpu(executorMinResourceCpu + "m")
        .executorMinCpu(sparkStaticConfiguration.getExecutorResources().getCpu().getMin())
        .executorMaxCpu(sparkStaticConfiguration.getExecutorResources().getCpu().getMax())
        .executorLimitMemory(sparkStaticConfiguration.getExecutorResources().getMemory().getLimitGb() + "Gi")
        // dynamicAllocation
        .initialExecutors(executorInstances(download))
        .minExecutors(sparkStaticConfiguration.getMinInstances())
        .maxExecutors(sparkStaticConfiguration.getMaxInstances())
        // Extra
        .callbackUrl(airflowConfiguration.getAirflowCallback())
        .build())
      .dagRunId(downloadDagId(download.getKey()))
      .build();
  }

  public String downloadDagId(String downloadKey) {
    return "download-" + downloadKey;
  }

  @Override
  public JobStatus create(String downloadKey) {
    try {
      Download download = downloadClient.get(downloadKey);
      JsonNode response = airflowRunner.createRun(getAirflowBody(download));

      asyncStatusCheck(download.getKey());

      log.info("Response {}", response);
      return JobStatus.RUNNING;
    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      return JobStatus.FAILED;
    }
  }

  @Override
  public JobStatus cancel(String downloadKey) {
    try {
      String dagId = downloadDagId(downloadKey);
      JsonNode jsonNode = airflowRunner.deleteRun(dagId);

      log.info("Airflow DAG {} has been stopped: {}", dagId, jsonNode);
      return JobStatus.CANCELLED;
    } catch (Exception ex) {
      log.error("Cancelling the download {}", downloadKey, ex);
      return JobStatus.FAILED;
    }
  }

  @SneakyThrows
  @Override
  public Optional<Status> getStatusByName(String downloadKey) {
    String dagId = downloadDagId(downloadKey);
    JsonNode jsonStatus = airflowRunner.getRun(dagId);
    String status = jsonStatus.get("state").asText();
    if ("queued".equalsIgnoreCase(status)) {
      return Optional.of(Status.PREPARING);
    }
    if ("running".equalsIgnoreCase(status) || "rescheduled".equalsIgnoreCase(status) || "retry".equalsIgnoreCase(status)) {
      return Optional.of(Status.RUNNING);
    }
    if ("success".equalsIgnoreCase(status)) {
      return Optional.of(Status.SUCCEEDED);
    }
    if ("failed".equalsIgnoreCase(status)) {
      return Optional.of(Status.FAILED);
    }
    return Optional.empty();
  }

  @Override
  public List<Download> renewRunningDownloadsStatuses(List<Download> downloads) {
    List<Download> result = new ArrayList<>(downloads.size());
    for (Download download : downloads) {
      String sparkAppName = normalize(download.getKey());
      Optional<Status> status = getStatusByName(sparkAppName);
      if (status.isPresent()) {
        download.setStatus(status.get());
      } else {
        log.warn("Can't find spark application status for the download {}", sparkAppName);
      }
      result.add(download);
    }
    return result;
  }

  /**
   * A lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.'.
   * Must start and end with an alphanumeric character and its max lentgh is 64 characters.
   */
  private static String normalize(String sparkAppName) {
    return "download-" + sparkAppName.toLowerCase().replace("_to_", "-").replace("_", "-");
  }

  private void asyncStatusCheck(String downloadKey) {
    CompletableFuture.runAsync(
        () -> {
          try {

            Optional<Status> status = getStatusByName(downloadKey);
            while (status.isPresent() && Status.SUCCEEDED != status.get() && Status.FAILED != status.get()) {
              TimeUnit.SECONDS.sleep(airflowConfiguration.apiCheckDelaySec);
              status = getStatusByName(downloadKey);
            }

            log.info("Spark Application {} is finished with status {}", downloadKey, status.get());
          } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
          } finally {
            lockerService.unlock(downloadKey);
          }
        });
  }
}
