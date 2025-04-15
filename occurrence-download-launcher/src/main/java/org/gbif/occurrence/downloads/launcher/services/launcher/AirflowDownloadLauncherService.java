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

import static org.gbif.api.model.occurrence.Download.Status.EXECUTING_STATUSES;
import static org.gbif.api.model.occurrence.Download.Status.FINISH_STATUSES;
import static org.gbif.api.model.occurrence.Download.Status.SUCCEEDED;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.occurrence.downloads.launcher.pojo.AirflowConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.SparkStaticConfiguration;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.gbif.occurrence.downloads.launcher.services.launcher.airflow.AirflowBody;
import org.gbif.occurrence.downloads.launcher.services.launcher.airflow.AirflowClient;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AirflowDownloadLauncherService implements DownloadLauncher {

  private static final Retry AIRFLOW_RETRY =
      Retry.of(
          "airflowApiCall",
          RetryConfig.custom()
              .maxAttempts(7)
              .intervalFunction(IntervalFunction.ofExponentialBackoff(Duration.ofSeconds(6)))
              .build());

  private final SparkStaticConfiguration sparkStaticConfiguration;
  private final AirflowClient bigDownloadsAirflowClient;
  private final AirflowClient smallDownloadsAirflowClient;
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
    this.bigDownloadsAirflowClient =
        buildAirflowClient(airflowConfiguration.bigDownloadsAirflowDagName);
    this.smallDownloadsAirflowClient =
        buildAirflowClient(airflowConfiguration.smallDownloadsAirflowDagName);
    this.lockerService = lockerService;
  }

  // NOTE: this has to match with the ElasticDownloadWorkflow#isSmallDownload method
  private boolean isSmallDownload(Download download) {
    return (download.getRequest().getFormat() == DownloadFormat.DWCA
            || download.getRequest().getFormat() == DownloadFormat.SIMPLE_CSV)
        && download.getTotalRecords() != -1
        && sparkStaticConfiguration.getSmallDownloadCutOff() >= download.getTotalRecords();
  }

  private long calculateExecutorInstances(Download download) {
    return isSmallDownload(download)
        ? sparkStaticConfiguration.getMinInstances()
        : Math.min(
            sparkStaticConfiguration.getMaxInstances(),
            Math.max(
                download.getTotalRecords() / sparkStaticConfiguration.getRecordsPerInstance(),
                sparkStaticConfiguration.getMinInstances()));
  }

  /** Calculate the executor memory limit based on the number of executor instances. */
  private long calculateExecutorMemoryLimit(long executorInstances) {
    // Get the memory per core in GB
    int memoryPerCoreGb = sparkStaticConfiguration.getMemoryPerCoreGb();

    // If memory per core is greater than 0, calculate the total memory limit
    if (memoryPerCoreGb > 0) {
      // Calculate the total memory overhead in MB
      int totalMemoryOverheadMb =
          sparkStaticConfiguration.getMemoryOverheadMb()
              + sparkStaticConfiguration.getVectorMemory();

      // Convert the total memory overhead to GB
      long totalMemoryOverheadGb = totalMemoryOverheadMb / 1024L;

      // Calculate the total memory limit
      long totalMemoryLimitGb = (memoryPerCoreGb * executorInstances) + totalMemoryOverheadGb;

      // If the total memory limit is less than the executor memory limit, return the total memory
      // limit
      return Math.min(
          totalMemoryLimitGb,
          sparkStaticConfiguration.getExecutorResources().getMemory().getLimitGb());
    }

    // If memory per core is 0 or less, return the executor memory limit
    return sparkStaticConfiguration.getExecutorResources().getMemory().getLimitGb();
  }

  private AirflowBody getAirflowBody(Download download) {

    int driverMemory =
        sparkStaticConfiguration.getDriverResources().getMemory().getLimitGb() * 1024;
    int driverCpu =
        Integer.parseInt(
            sparkStaticConfiguration.getDriverResources().getCpu().getMin().replace("m", ""));
    int executorMemory =
        sparkStaticConfiguration.getExecutorResources().getMemory().getLimitGb() * 1024;
    int executorCpu =
        Integer.parseInt(
            sparkStaticConfiguration.getExecutorResources().getCpu().getMin().replace("m", ""));
    int memoryOverhead = sparkStaticConfiguration.getMemoryOverheadMb();
    // Given as megabytes (Mi)
    int vectorMemory = sparkStaticConfiguration.getVectorMemory();
    // Given as whole CPUs
    int vectorCpu = sparkStaticConfiguration.getVectorCpu();
    // Calculate values for Yunikorn annotation
    // Driver
    int driverMinResourceMemory =
        Double.valueOf(
                Math.ceil(
                    (driverMemory
                            + vectorMemory
                            + sparkStaticConfiguration.getDriverMemoryOverheadMb())
                        / 1024d))
            .intValue();
    int driverMinResourceCpu = driverCpu + vectorCpu;
    // Executor
    int executorMinResourceMemory =
        Double.valueOf(Math.ceil((executorMemory + memoryOverhead + vectorMemory) / 1024d))
            .intValue();
    int executorMinResourceCpu = executorCpu + vectorCpu;

    long executorInstances = calculateExecutorInstances(download);
    return AirflowBody.builder()
        .conf(
            AirflowBody.Conf.builder()
                .args(
                    Lists.newArrayList(
                        download.getKey(),
                        download.getRequest().getType().getCoreTerm().name(),
                        "/stackable/spark/jobs/download.properties"))
                // Driver
                .driverMinCpu(sparkStaticConfiguration.getDriverResources().getCpu().getMin())
                .driverMaxCpu(sparkStaticConfiguration.getDriverResources().getCpu().getMax())
                .driverLimitMemory(
                    sparkStaticConfiguration.getDriverResources().getMemory().getLimitGb() + "Gi")
                .driverMinResourceMemory(driverMinResourceMemory + "Gi")
                .driverMinResourceCpu(driverMinResourceCpu + "m")
                .driverMemoryOverhead(String.valueOf(sparkStaticConfiguration.getDriverMemoryOverheadMb()))
                // Executor
                .memoryOverhead(String.valueOf(sparkStaticConfiguration.getMemoryOverheadMb()))
                .executorMinResourceMemory(executorMinResourceMemory + "Gi")
                .executorMinResourceCpu(executorMinResourceCpu + "m")
                .executorMinCpu(sparkStaticConfiguration.getExecutorResources().getCpu().getMin())
                .executorMaxCpu(sparkStaticConfiguration.getExecutorResources().getCpu().getMax())
                .executorLimitMemory(calculateExecutorMemoryLimit(executorInstances) + "Gi")
                // dynamicAllocation
                .minExecutors(sparkStaticConfiguration.getMinInstances())
                .maxExecutors(executorInstances)
                // Extra
                .gbifApiUrl(airflowConfiguration.getGbifApiUrl())
                .build())
        .dagRunId(downloadDagId(download.getKey()))
        .build();
  }

  public String downloadDagId(String downloadKey) {
    return "download-" + downloadKey;
  }

  @Override
  public JobStatus createRun(String downloadKey) {
    try {
      Download download = downloadClient.get(downloadKey);
      Optional<Status> status = getStatusByName(download);

      // Send task to Airflow is no statuses found
      if (status.isEmpty()) {
        JsonNode response =
            Retry.<AirflowBody, JsonNode>decorateFunction(
                    AIRFLOW_RETRY, body -> getAirflowClient(download).createRun(body))
                .apply(getAirflowBody(download));
        log.info("Create a run for: {}", response);
      } else if (status.get() == SUCCEEDED) {
        log.info("downloadKey {} is already has finished statuses", downloadKey);
        return JobStatus.FINISHED;
      } else if (FINISH_STATUSES.contains(status.get())) {
        log.info(
            "downloadKey {} is already in one of failed statuses: {}", downloadKey, status.get());
        return JobStatus.FAILED;
      } else if (EXECUTING_STATUSES.contains(status.get())) {
        log.info(
            "downloadKey {} is already in one of running statuses: {}", downloadKey, status.get());
      }

      asyncStatusCheck(download);
      return JobStatus.RUNNING;
    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      return JobStatus.FAILED;
    }
  }

  @Override
  public JobStatus cancelRun(String downloadKey) {
    try {
      Download download = downloadClient.get(downloadKey);
      String dagId = downloadDagId(downloadKey);

      JsonNode cancelledJsonNode =
          Retry.<String, JsonNode>decorateFunction(
                  AIRFLOW_RETRY, dagRunId -> getAirflowClient(download).setCancelledNote(dagRunId))
              .apply(dagId);
      log.info("Airflow DAG {} has been noted as cancelled: {}", dagId, cancelledJsonNode);

      JsonNode failedJsonNode =
          Retry.<String, JsonNode>decorateFunction(
                  AIRFLOW_RETRY, dagRunId -> getAirflowClient(download).failRun(dagRunId))
              .apply(dagId);
      log.info("Airflow DAG {} has been marked as failed: {}", dagId, failedJsonNode);

      return JobStatus.CANCELLED;
    } catch (Exception ex) {
      log.error("Cancelling the download {}", downloadKey, ex);
      return JobStatus.FAILED;
    }
  }

  @SneakyThrows
  @Override
  public Optional<Status> getStatusByName(String downloadKey) {
    Download download = downloadClient.get(downloadKey);
    return getStatusByName(download);
  }

  private Optional<Status> getStatusByName(Download download) {
    String dagId = downloadDagId(download.getKey());
    JsonNode jsonStatus =
        Retry.<String, JsonNode>decorateFunction(
                AIRFLOW_RETRY, dagRunId -> getAirflowClient(download).getRun(dagRunId))
            .apply(dagId);
    JsonNode state = jsonStatus.get("state");

    if (state == null) {
      return Optional.empty();
    }

    switch (state.asText()) {
      case "queued":
      case "scheduled":
      case "rescheduled":
      case "up_for_retry":
      case "up_for_reschedule":
        return Optional.of(Status.PREPARING);
      case "running":
      case "restarting":
      case "retry":
        return Optional.of(Status.RUNNING);
      case "success":
        return Optional.of(SUCCEEDED);
      case "failed":
      case "removed":
      case "upstream_failed":
        return Optional.of(Status.FAILED);
      default:
        return Optional.empty();
    }
  }

  @Override
  public List<Download> renewRunningDownloadsStatuses(List<Download> downloads) {
    List<Download> result = new ArrayList<>(downloads.size());
    for (Download download : downloads) {
      Optional<Status> status = getStatusByName(download);
      if (status.isPresent() && download.getStatus() != status.get()) {
        log.info(
            "Update download {} status from {} to {}",
            download.getKey(),
            download.getStatus(),
            status.get());
        download.setStatus(status.get());
        result.add(download);
      } else if (status.isEmpty()) {
        log.warn("Can't find spark application status for the download {}", download.getKey());
      } else if (download.getStatus() == status.get()) {
        log.info("download key {} status didn't change", download.getKey());
      }
    }
    return result;
  }

  private void asyncStatusCheck(Download download) {
    CompletableFuture.runAsync(
        () -> {
          try {

            Optional<Status> status = getStatusByName(download);
            while (status.isPresent()
                && SUCCEEDED != status.get()
                && Status.FAILED != status.get()) {
              TimeUnit.SECONDS.sleep(airflowConfiguration.apiCheckDelaySec);
              status = getStatusByName(download);
            }

            log.info(
                "Spark Application {} is finished with status {}",
                download.getKey(),
                status.orElse(null));
          } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
          } finally {
            lockerService.unlock(download.getKey());
          }
        });
  }

  private AirflowClient getAirflowClient(Download download) {
    return isSmallDownload(download) ? smallDownloadsAirflowClient : bigDownloadsAirflowClient;
  }

  private AirflowClient buildAirflowClient(String dagName) {
    return AirflowClient.builder()
        .airflowAddress(airflowConfiguration.airflowAddress)
        .airflowUser(airflowConfiguration.airflowUser)
        .airflowPass(airflowConfiguration.airflowPass)
        .airflowDagName(dagName)
        .build();
  }
}
