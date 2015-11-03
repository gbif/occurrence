package org.gbif.occurrence.cli.dataset.service;

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.occurrence.cli.common.GangliaConfiguration;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Objects;

public class DatasetMutationConfiguration {

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
  public int msgPoolSize = 10;

  @Parameter(names = "--delete-dataset-queue-name")
  @NotNull
  public String deleteDatasetQueueName;

  @Parameter(names = "--interpret-dataset-queue-name")
  @NotNull
  public String interpretDatasetQueueName;

  @Parameter(names = "--parse-dataset-queue-name")
  @NotNull
  public String parseDatasetQueueName;

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("messaging", messaging).add("ganglia", ganglia).add("hbase", hbase)
      .add("msgPoolSize", msgPoolSize).add("deleteDatasetQueueName", deleteDatasetQueueName)
      .add("interpretDatasetQueueName", interpretDatasetQueueName).add("parseDatasetQueueName", parseDatasetQueueName)
      .toString();
  }
}
