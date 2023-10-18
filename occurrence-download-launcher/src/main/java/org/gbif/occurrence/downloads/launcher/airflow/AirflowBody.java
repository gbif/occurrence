package org.gbif.occurrence.downloads.launcher.airflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class AirflowBody {



  @JsonProperty("dag_run_id")
  private final String dagRunId;

  private final Conf conf;

  @Data
  @Builder
  public static class Conf {

    private final List<String> args;
    private final String driverCores;
    private final String driverMemory;
    private final int executorInstances;
    private final String executorCores;
    private final String executorMemory;

    private final String version = "1.0.12";
    private final String component = "occurrence-download-spark";
    private final String main = "org.gbif.occurrence.download.spark.SparkDownloads";

    private final String hdfsClusterName = "gbif-hdfs";
    private final String hiveClusterName = "gbif-hive-metastore";
    private final String hbaseClusterName = "gbif-hbase";
    private final String componentConfig = "occurrence";
    private final String callbackUrl;
  }
}
