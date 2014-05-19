package org.gbif.occurrence.cli.dataset.service;

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.occurrence.cli.common.GangliaConfiguration;
import org.gbif.occurrence.cli.common.ZooKeeperConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

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
  public ZooKeeperConfiguration zooKeeper = new ZooKeeperConfiguration();

  @Parameter(names = "--msg-pool-size")
  @Min(1)
  public int msgPoolSize = 10;

  @Parameter(names = "--hbase-pool-size")
  @Min(1)
  public int hbasePoolSize = 20;

  @Parameter(names = "--delete-dataset-queue-name")
  @NotNull
  public String deleteDatasetQueueName;

  @Parameter(names = "--interpret-dataset-queue-name")
  @NotNull
  public String interpretDatasetQueueName;

  @Parameter(names = "--parse-dataset-queue-name")
  @NotNull
  public String parseDatasetQueueName;

  @Parameter(names = "--occ-table")
  @NotNull
  public String occTable;

  @Parameter(names = "--occ-counter-table")
  @NotNull
  public String counterTable;

  @Parameter(names = "--occ-lookup-table")
  @NotNull
  public String lookupTable;
}
