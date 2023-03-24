package org.gbif.occurrence.downloads.launcher.services;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.occurrence.downloads.launcher.DownloadsMessage;
import org.gbif.occurrence.downloads.launcher.config.SparkConfiguration;
import org.gbif.occurrence.downloads.launcher.services.YarnClientService.Application;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;
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
  public JobStatus createJob(@NotNull DownloadsMessage message) {
    try {
      String jobId = message.getJobId();

      new SparkLauncher()
        .setAppName(jobId)
        .setSparkHome(sparkConfiguration.getSparkHome())
        .setDeployMode(sparkConfiguration.getDeployMode())
        .setMaster(sparkConfiguration.getMaster())
        .setAppResource(sparkConfiguration.getAppResource())
        .setMainClass(sparkConfiguration.getMainClass())
        .startApplication(outputListener);

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
