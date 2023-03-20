package org.gbif.occurrence.downloads.launcher.services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.gbif.occurrence.downloads.launcher.DownloadsMessage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.Listener;
import org.apache.spark.launcher.SparkLauncher;

import javax.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class YarnJobManagerService implements JobManager {

  private static final EnumSet<YarnApplicationState> YARN_APPLICATION_STATES =
    EnumSet.of(
      YarnApplicationState.NEW,
      YarnApplicationState.NEW_SAVING,
      YarnApplicationState.SUBMITTED,
      YarnApplicationState.ACCEPTED,
      YarnApplicationState.RUNNING);

  private static final Set<String> APPLICATION_TYPES = Collections.singleton("SPARK");

  private final YarnClient yarnClient;

  public YarnJobManagerService(String pathToYarnSite, String pathToCoreSite, String pathToHdfsSite) {
    this.yarnClient = createYarnClient(pathToYarnSite, pathToCoreSite, pathToHdfsSite);
  }

  @Override
  public Optional<String> createJob(@NotNull DownloadsMessage message) {
    try {
      String jobId = message.getJobId();

      new SparkLauncher()
        .setAppName(jobId)
        .setSparkHome("/opt/cloudera/parcels/CDH-5.16.2-1.cdh5.16.2.p0.8/lib/spark")
        .setDeployMode("cluster")
        .setMaster("yarn")
        .setAppResource("hdfs://ha-nn/pipelines/jars/ingest-gbif.jar")
        .setMainClass("org.gbif.pipelines.ingest.pipelines.VerbatimToIdentifierPipeline")
        .startApplication(new Listener() {
          @Override
          public void stateChanged(SparkAppHandle sparkAppHandle) {

          }

          @Override
          public void infoChanged(SparkAppHandle sparkAppHandle) {

          }
        });

      return getApplicationIdByName(jobId).stream().findAny().map(Objects::toString);
    } catch (Exception ex) {
      log.error("Oops", ex);
    }
    return Optional.empty();
  }

  @Override
  public void cancelJob(@NotNull String jobId) {
    try {
      for (ApplicationId applicationId : getApplicationIdByName(jobId)) {
        log.info("Killing jobId {}, aplicationId: {}", jobId, applicationId);
        yarnClient.killApplication(applicationId);
      }
    } catch (YarnException | IOException ex) {
      log.error("Exception during the killing the jobId {}", jobId, ex);
    }
  }

  // TODO: move to Spring bean
  @Override
  public void close() {
    try {
      if (yarnClient != null) {
        yarnClient.close();
      }
    } catch (IOException ex) {
      log.error("Exception during the closing YARN client", ex);
    }
  }

  private List<ApplicationId> getApplicationIdByName(@NotNull String jobId) {
    List<ApplicationId> ids = new ArrayList<>();
    try {
      for (ApplicationReport ar : yarnClient.getApplications(APPLICATION_TYPES, YARN_APPLICATION_STATES)) {
        if (ar.getName().equals(jobId)) {
          ApplicationId applicationId = ar.getApplicationId();
          ids.add(applicationId);
        }
      }
    } catch (YarnException | IOException ex) {
      log.error("Exception during the killing the jobId {}", jobId, ex);
    }
    return ids;
  }

  // TODO: move to Spring bean
  private YarnClient createYarnClient(String pathToYarnSite, String pathToCoreSite, String pathToHdfsSite) {
    Configuration cfg = new Configuration();
    cfg.addResource(new Path(pathToYarnSite));
    cfg.addResource(new Path(pathToCoreSite));
    cfg.addResource(new Path(pathToHdfsSite));

    YarnConfiguration configuration = new YarnConfiguration(cfg);
    YarnClient client = YarnClient.createYarnClient();
    client.init(configuration);
    client.start();

    return client;
  }
}
