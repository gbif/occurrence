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
package org.gbif.occurrence.downloads.launcher.services.launcher.stackable;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.occurrence.downloads.launcher.airflow.AirflowBody;
import org.gbif.occurrence.downloads.launcher.airflow.AirflowRunner;
import org.gbif.occurrence.downloads.launcher.pojo.AirflowConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.SparkStaticConfiguration;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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


  public AirflowDownloadLauncherService(
    SparkStaticConfiguration sparkStaticConfiguration,
    AirflowConfiguration airflowConfiguration,
    OccurrenceDownloadClient downloadClient) {
    this.sparkStaticConfiguration = sparkStaticConfiguration;
    this.downloadClient = downloadClient;
    this.airflowConfiguration = airflowConfiguration;
    airflowRunner = AirflowRunner.builder().airflowConfiguration(airflowConfiguration).build();
  }


  private boolean isSmallDownload(Download download) {
    return sparkStaticConfiguration.getSmallDownloadCutOff() >= download.getTotalRecords();
  }

  private int executorInstances(Download download) {
     return isSmallDownload(download)? 1 : Math.min(sparkStaticConfiguration.getLargeDownloads().getMaxInstances(),
       Math.max((int)download.getTotalRecords() / sparkStaticConfiguration.getLargeDownloads().getRecordsPerInstance(), 1));
  }

  private SparkStaticConfiguration.DownloadSparkConfiguration getDownloadSparkSettings(Download download) {
    return isSmallDownload(download)? sparkStaticConfiguration.getSmallDownloads() : sparkStaticConfiguration.getLargeDownloads();
  }

  private AirflowBody getAirflowBody(Download download) {
    SparkStaticConfiguration.DownloadSparkConfiguration sparkConfiguration = getDownloadSparkSettings(download);
    return AirflowBody.builder()
      .conf(AirflowBody.Conf.builder()
        .args(Lists.newArrayList(download.getKey(), download.getRequest().getType().getCoreTerm().name(), "/stackable/spark/jobs/download.properties"))
        .driverCores(sparkConfiguration.getDriverResources().getCpu().getMax())
        .driverMemory(sparkConfiguration.getDriverResources().getMemory().getLimit())
        .executorCores(sparkConfiguration.getExecutorResources().getCpu().getMax())
        .executorMemory(sparkConfiguration.getExecutorResources().getMemory().getLimit())
        .executorInstances(executorInstances(download))
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
    String status = jsonStatus.get("status").asText();
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
}
