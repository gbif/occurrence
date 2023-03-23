package org.gbif.occurrence.downloads.launcher.services;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.occurrence.downloads.launcher.DownloadsMessage;
import org.gbif.occurrence.downloads.launcher.config.SparkConfiguration;
import org.gbif.occurrence.downloads.launcher.services.YarnClientService.Application;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.stereotype.Service;

import javax.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("yarn")
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
  public Optional<String> createJob(@NotNull DownloadsMessage message) {
    try {
      String jobId = message.getJobId();

      SparkAppHandle application =
        new SparkLauncher()
          .setAppName(jobId)
          .setSparkHome(sparkConfiguration.getSparkHome())
          .setDeployMode(sparkConfiguration.getDeployMode())
          .setMaster(sparkConfiguration.getMaster())
          .setAppResource(sparkConfiguration.getAppResource())
          .setMainClass(sparkConfiguration.getMainClass())
          .startApplication(outputListener);

      String applicationId = application.getAppId();

      while (applicationId == null) {
        long waitTimeout = sparkConfiguration.getWaitTimeout();
        log.info("Waiting {} seconds for console output to retrieve applicationId...", waitTimeout);
        TimeUnit.SECONDS.sleep(waitTimeout);
        applicationId = application.getAppId();
      }

      return Optional.of(applicationId);
    } catch (Exception ex) {
      log.error("Oops", ex);
    }
    return Optional.empty();
  }

  @Override
  public void cancelJob(@NotNull String jobId) {
    yarnClientService.killApplicationByName(jobId);
  }

  @Override
  public List<Download> renewRunningDownloadsStatuses(List<Download> downloads) {

    Set<String> keySet = downloads.stream().map(Download::getKey).collect(Collectors.toSet());
    Map<String, Application> applications = yarnClientService.getAllApplicationByNames(keySet);

    List<Download> result = new ArrayList<>();
    for (Download download : downloads) {
      String downloadKey = download.getKey();
      Application application = applications.get(downloadKey);
      if (application == null) {
        download.setStatus(Status.FAILED);
        result.add(download);
      } else if (application.isFinished()) {

        Status status = Status.FAILED;
        YarnApplicationState state = application.getState();

        if (state.equals(YarnApplicationState.FINISHED)) {
          status = Status.SUCCEEDED;
        } else if (state.equals(YarnApplicationState.KILLED)) {
          status = Status.CANCELLED;
        }

        download.setStatus(status);
        result.add(download);
      } else {
        log.info(
          "Downloads with downloadKey {} has applicationId {} and status {}",
          downloadKey,
          application.getApplicationId(),
          application.getState().toString());
      }
    }
    return result;
  }
}
