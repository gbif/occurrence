package org.gbif.occurrence.downloads.launcher.config;

import org.gbif.common.messaging.config.GangliaConfiguration;
import org.gbif.common.messaging.config.MessagingConfiguration;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.Data;

@Data
public class DownloadServiceConfiguration {

  @ParametersDelegate
  @NotNull
  @Valid
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @ParametersDelegate @Valid @NotNull
  public GangliaConfiguration ganglia = new GangliaConfiguration();

  @Parameter(names = "--pool-size")
  @Min(1)
  public int poolSize = 1;

  @Parameter(names = "--queue-name")
  @NotNull
  public String queueName;

  @Parameter(names = "--dead-queue-name")
  @NotNull
  public String deadQueueName;
}
