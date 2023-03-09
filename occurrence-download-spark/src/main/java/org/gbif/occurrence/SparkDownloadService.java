package org.gbif.occurrence;

import org.gbif.api.model.occurrence.DownloadRequest;

import java.io.Closeable;
import java.io.IOException;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.sql.SparkSession;

public class SparkDownloadService implements Closeable {

  private SparkSession sparkSession;

  public void kill(String jobId) {
    sparkSession.sparkContext().cancelJobGroup(jobId);
  }

  public String createJob(DownloadRequest downloadRequest) {
    SparkLauncher sparkLauncher;
    //sparkLauncher.
    return "";
  }

  @Override
  public void close() throws IOException {
    sparkSession.close();
  }

  private SparkSession createSparkSession(String jobId) {
    SparkSession.Builder sparkBuilder = SparkSession.builder()
      .appName(jobId)
      //.config("spark.sql.warehouse.dir", configuration.getWarehouseLocation())
      .enableHiveSupport();
    return sparkBuilder.getOrCreate();
  }
}
