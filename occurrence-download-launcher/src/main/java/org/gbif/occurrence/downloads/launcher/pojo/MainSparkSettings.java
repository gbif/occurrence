package org.gbif.occurrence.downloads.launcher.pojo;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MainSparkSettings {

  private int parallelism;

  private String executorMemory;

  private int executorNumbers;
}
