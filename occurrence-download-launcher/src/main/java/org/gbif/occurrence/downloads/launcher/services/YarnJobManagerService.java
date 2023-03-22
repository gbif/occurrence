package org.gbif.occurrence.downloads.launcher.services;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.gbif.occurrence.downloads.launcher.DownloadsMessage;
import org.gbif.occurrence.downloads.launcher.config.SparkConfiguration;

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

  public YarnJobManagerService(YarnClientService yarnClientService, SparkConfiguration sparkConfiguration, SparkOutputListener outputListener) {
    this.yarnClientService = yarnClientService;
    this.sparkConfiguration = sparkConfiguration;
    this.outputListener = outputListener;
  }

  @Override
  public Optional<String> createJob(@NotNull DownloadsMessage message) {
    try {
      String jobId = message.getJobId();

      SparkAppHandle application = new SparkLauncher()
          .setAppName(jobId)
          .setSparkHome(sparkConfiguration.getSparkHome())
          .setDeployMode(sparkConfiguration.getDeployMode())
          .setMaster(sparkConfiguration.getMaster())
          .setAppResource(sparkConfiguration.getAppResource())
          .setMainClass(sparkConfiguration.getMainClass())
          .startApplication(outputListener);

      String applicationId = application.getAppId();

      while (applicationId == null){
        log.info("Wait 3 seconds...");
        TimeUnit.SECONDS.sleep(3L);
        applicationId = application.getAppId();
      }

      return Optional.ofNullable(applicationId);
    } catch (Exception ex) {
      log.error("Oops", ex);
    }
    return Optional.empty();
  }

  @Override
  public void cancelJob(@NotNull String jobId) {
    yarnClientService.killApplicationByName(jobId);
    //TODO: Update status for the download using registry-ws
  }
}
