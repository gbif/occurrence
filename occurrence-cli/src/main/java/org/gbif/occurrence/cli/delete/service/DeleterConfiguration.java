package org.gbif.occurrence.cli.delete.service;

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.occurrence.cli.common.GangliaConfiguration;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Objects;

public class DeleterConfiguration {

  @ParametersDelegate
  @NotNull
  @Valid
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @ParametersDelegate
  @Valid
  @NotNull
  public GangliaConfiguration ganglia = new GangliaConfiguration();

  @ParametersDelegate
  @Valid
  @NotNull
  public OccHBaseConfiguration hbase = new OccHBaseConfiguration();

  @Parameter(names = "--msg-pool-size")
  @Min(1)
  public int msgPoolSize = 1;

  @Parameter(names = "--queue-name")
  @NotNull
  public String queueName;

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("messaging", messaging).add("ganglia", ganglia).add("hbase", hbase)
      .add("msgPoolSize", msgPoolSize).add("queueName", queueName).toString();
  }
}
