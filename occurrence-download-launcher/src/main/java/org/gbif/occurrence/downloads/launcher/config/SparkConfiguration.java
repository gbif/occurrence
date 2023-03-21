package org.gbif.occurrence.downloads.launcher.config;

import javax.validation.constraints.NotNull;
import lombok.Data;

@Data
public class SparkConfiguration {

  @NotNull
  private String sparkHome;

  @NotNull
  private String appResource;

  @NotNull
  private String mainClass;
}

