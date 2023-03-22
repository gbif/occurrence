package org.gbif.occurrence.downloads.launcher.config;

import javax.validation.constraints.NotNull;
import lombok.Data;

@Data
public class DownloadServiceConfiguration {

  @NotNull
  private String queueName;

  @NotNull
  private String deadQueueName;

  @NotNull
  private String pathToYarnSite;

  @NotNull
  private String cron;
}

