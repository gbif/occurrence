package org.gbif.occurrence.downloads.launcher.airflow;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class AirflowBody {

  private final List<String> args;
  private final String driverCores;
  private final String driverMemory;
  private final int executorInstances;
  private final String executorCores;
  private final String executorMemory;

}
