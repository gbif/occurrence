package org.gbif.occurrence.downloads.launcher.pojo;

import lombok.Data;

@Data
public class SparkConfiguration {

  public int recordsPerThread;

  public int parallelismMin;

  public int parallelismMax;

  public int memoryOverhead;

  public int executorMemoryGbMin;

  public int executorMemoryGbMax;

  public int executorCores;

  public int executorNumbersMin;

  public int executorNumbersMax;

  public int driverCores;

  public String driverMemory;
}
