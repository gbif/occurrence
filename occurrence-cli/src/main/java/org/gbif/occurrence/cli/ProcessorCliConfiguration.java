package org.gbif.occurrence.cli;

import org.gbif.occurrence.cli.common.GangliaConfiguration;
import org.gbif.occurrence.processor.guice.ProcessorConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Objects;

public class ProcessorCliConfiguration extends ProcessorConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public GangliaConfiguration ganglia = new GangliaConfiguration();

  @Parameter(names = "--msg-pool-size")
  @Min(1)
  public int msgPoolSize = 10;

  @Parameter(names = "--primary-queue-name")
  @NotNull
  public String primaryQueueName;

  @Parameter(names = "--secondary-queue-name")
  public String secondaryQueueName;

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("super", super.toString())
      .add("msgPoolSize", msgPoolSize)
      .add("primaryQueueName", primaryQueueName)
      .add("secondaryQueueName", secondaryQueueName)
      .toString();
  }
}
