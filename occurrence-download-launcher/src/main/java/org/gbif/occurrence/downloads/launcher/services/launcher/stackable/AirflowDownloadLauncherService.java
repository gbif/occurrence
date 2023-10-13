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

import io.kubernetes.client.openapi.ApiException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.occurrence.downloads.launcher.airflow.AirflowBody;
import org.gbif.occurrence.downloads.launcher.pojo.AirflowConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.SparkDynamicSettings;
import org.gbif.occurrence.downloads.launcher.pojo.SparkStaticConfiguration;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher;
import org.gbif.stackable.K8StackableSparkController.Phase;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.gbif.stackable.K8StackableSparkController.NOT_FOUND;

@Slf4j
@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class AirflowDownloadLauncherService implements DownloadLauncher {
  private final LockerService lockerService;

  private final SparkStaticConfiguration sparkStaticConfiguration;


  public AirflowDownloadLauncherService(
      SparkStaticConfiguration sparkStaticConfiguration,
      LockerService lockerService) {
    this.lockerService = lockerService;
    this.sparkStaticConfiguration = sparkStaticConfiguration;
  }


  private boolean isSmallDownload(Download download) {
    return sparkStaticConfiguration.getSmallDownloadCutOff() >= download.getTotalRecords();
  }

  private int executorInstances(Download download) {
     return isSmallDownload(download)? 0 : Math.min(sparkStaticConfiguration.getLargeDownloads().getMaxInstances(),
       (int)download.getTotalRecords() / sparkStaticConfiguration.getLargeDownloads().getRecordsPerInstance());
  }

  private SparkStaticConfiguration.DownloadSparkConfiguration getDownloadSparkSettings(Download download) {
    return isSmallDownload(download)? sparkStaticConfiguration.getSmallDownloads() : sparkStaticConfiguration.getLargeDownloads();
  }

  private AirflowConfiguration getAirflowConfiguration(Download download) {

  }

  public String downloadDagId(Download download) {
    return "download-" + download.getKey();
  }

  @Override
  public JobStatus create(Download download) {

    try {
      String sparkAppName = normalize(download.getKey());


      // TODO Calculate spark settings
      SparkDynamicSettings sparkSettings =
          SparkDynamicSettings.builder()
              .executorInstances(executorInstances(download))
              .sparkAppName(sparkAppName)
              .downloadsKey(download.getKey())
              .build();

      AirflowBody.builder().

      SparkCrd sparkCrd = sparkCrdService.createSparkCrd(sparkSettings, getDownloadSparkSettings(download));

      airflowRunner.createRun(downloadDagId(download), sparkCrd);

      asyncStatusCheck(download.getKey(), sparkAppName);

      return JobStatus.RUNNING;
    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      return JobStatus.FAILED;
    }
  }

  @Override
  public JobStatus cancel(String downloadKey) {
    try {
      String sparkAppName = normalize(downloadKey);
      sparkController.stopSparkApplication(sparkAppName);
      log.info("Spark application {} has been stopped", sparkAppName);
      return JobStatus.CANCELLED;
    } catch (ApiException ex) {
      log.error("Cancellig the download {}", downloadKey, ex);
      return JobStatus.FAILED;
    }
  }

  @SneakyThrows
  @Override
  public Optional<Status> getStatusByName(String downloadKey) {
    String sparkAppName = normalize(downloadKey);
    try {
      Phase phase = sparkController.getApplicationPhase(sparkAppName);
      if (phase == Phase.RUNNING) {
        return Optional.of(Status.RUNNING);
      }
      if (phase == Phase.SUCCEEDED) {
        return Optional.of(Status.SUCCEEDED);
      }
    } catch (ApiException ex) {
      if (ex.getCode() != NOT_FOUND) {
        throw ex;
      }
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

  private void asyncStatusCheck(String downloadKey, String sparkAppName) {
    CompletableFuture.runAsync(
        () -> {
          try {

            Phase phase = sparkController.getApplicationPhase(sparkAppName);
            while (Phase.SUCCEEDED != phase && Phase.FAILED != phase) {
              TimeUnit.SECONDS.sleep(stackableConfiguration.apiCheckDelaySec);
              phase = sparkController.getApplicationPhase(sparkAppName);
            }

            log.info("Spark Application {} is finished with status {}", sparkAppName, phase);
          } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
          } finally {
            lockerService.unlock(downloadKey);

            if (stackableConfiguration.deletePodsOnFinish) {
              try {
                sparkController.stopSparkApplication(sparkAppName);
              } catch (ApiException ex) {
                log.error("Can't stop Spark application {}", sparkAppName, ex);
              }
            }
          }
        });
  }

  /**
   * A lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.'.
   * Must start and end with an alphanumeric character and its max lentgh is 64 characters.
   */
  private static String normalize(String sparkAppName) {
    return "download-" + sparkAppName.toLowerCase().replace("_to_", "-").replace("_", "-");
  }
}
