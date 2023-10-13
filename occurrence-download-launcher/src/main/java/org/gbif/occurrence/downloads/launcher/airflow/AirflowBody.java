package org.gbif.occurrence.downloads.launcher.airflow;

import lombok.Builder;
import lombok.Data;
import org.gbif.occurrence.downloads.launcher.pojo.AirflowConfiguration;

import java.util.List;

@Data
public class AirflowBody {
  private final List<String> args;
  private final String driverCores;
  private final String driverMemory;
  private final int executorInstances;
  private final String executorCores;
  private final String executorMemory;
  private final String clusterName;

  @Builder
  public AirflowBody(AirflowConfiguration conf, List<String> args) {
    this.clusterName = conf.airflowCluster;
    this.driverCores = conf.maxCoresDriver;
    this.driverMemory = conf.maxMemoryDriver;
    this.executorCores = conf.maxCoresWorkers;
    this.executorMemory = conf.maxMemoryWorkers;
    this.executorInstances = conf.numberOfWorkers;
    this.args = args;
  }
}
