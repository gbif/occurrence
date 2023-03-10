package org.gbif.occurrence;

import java.io.IOException;
import java.util.Optional;

import org.gbif.api.model.occurrence.DownloadRequest;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.spark.sql.SparkSession;

public class YarnJobManager implements JobManager {

  private final YarnClient yarnClient;

  public YarnJobManager() {
    this.yarnClient = createYarnClient();
  }

  @Override
  public String createJob(String jobId, DownloadRequest downloadRequest) {
    return SparkSession.builder()
      .appName(jobId)
      //.config("spark.sql.warehouse.dir", configuration.getWarehouseLocation())
      .enableHiveSupport()
      .getOrCreate()
      .sparkContext()
      .applicationId();
  }

  @Override
  public void cancelJob(String jobId) {
    try {
      Optional<ApplicationId> applicationId = yarnClient.getApplications().stream()
        .filter(ar -> ar.getName().equals(jobId))
        .findAny()
        .map(ApplicationReport::getApplicationId);
      if (applicationId.isPresent()) {
        yarnClient.killApplication(applicationId.get());
      }
    } catch (YarnException | IOException ex) {
      // TODO: LOG
    }
  }

  @Override
  public void close() {
    try {
      yarnClient.close();
    } catch (IOException e) {
      // TODO: LOG
    }
  }

  // TODO: move to Spring bean
  private YarnClient createYarnClient() {
    YarnConfiguration conf = new YarnConfiguration();
    YarnClient client = YarnClient.createYarnClient();
    client.init(conf);
    client.start();
    return client;
  }
}
