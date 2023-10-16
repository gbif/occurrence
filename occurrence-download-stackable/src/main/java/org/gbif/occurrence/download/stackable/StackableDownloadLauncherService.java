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
package org.gbif.occurrence.download.stackable;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.occurrence.download.stackable.config.LauncherConfiguration;
import org.gbif.occurrence.download.stackable.config.SparkDynamicSettings;
import org.gbif.occurrence.download.stackable.config.StackableConfiguration;
import org.gbif.stackable.K8StackableSparkController;
import org.gbif.stackable.K8StackableSparkController.Phase;
import org.gbif.stackable.SparkCrd;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.util.KubeConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.stackable.K8StackableSparkController.NOT_FOUND;

@Slf4j
public class StackableDownloadLauncherService implements DownloadLauncher {

  private final StackableConfiguration stackableConfiguration;
  private final K8StackableSparkController sparkController;
  private final SparkCrdFactoryService sparkCrdService;

  public StackableDownloadLauncherService(
      StackableConfiguration stackableConfiguration,
      K8StackableSparkController sparkController,
      SparkCrdFactoryService sparkCrdService) {
    this.stackableConfiguration = stackableConfiguration;
    this.sparkController = sparkController;
    this.sparkCrdService = sparkCrdService;
  }

  @Override
  public JobStatus create(String downloadKey) {

    try {
      String sparkAppName = normalize(downloadKey);

      // TODO Calculate spark settings
      SparkDynamicSettings sparkSettings =
          SparkDynamicSettings.builder()
              .executorMemory("4")
              .parallelism(4)
              .executorNumbers(2)
              .sparkAppName(sparkAppName)
              .downloadsKey(downloadKey)
              .build();

      SparkCrd sparkCrd = sparkCrdService.createSparkCrd(sparkSettings);

      sparkController.submitSparkApplication(sparkCrd);

      asyncStatusCheck(downloadKey, sparkAppName);

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


  public static void main(String[] args) throws Exception {
    LauncherConfiguration configuration = LauncherConfiguration.fromYaml(args[0]);
    SparkCrd sparkCrd = SparkCrd.fromYaml(Files.newInputStream(Paths.get(configuration.stackable.sparkCrdConfigFile)));
    K8StackableSparkController controller = new K8StackableSparkController(sparkCrd, KubeConfig.loadKubeConfig(Files.newBufferedReader(Paths.get(configuration.stackable.kubeConfigFile))));
    SparkCrdFactoryService sparkCrdFactoryService = new SparkCrdFactoryService(configuration.distributed, configuration.spark, configuration.stackable);
    StackableDownloadLauncherService service = new StackableDownloadLauncherService(configuration.stackable, controller, sparkCrdFactoryService);
    JobStatus jobStatus = service.create(args[1]);
    System.out.println("Job " + args[1] + " started with status " + jobStatus);
  }
}
