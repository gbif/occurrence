package org.gbif.occurrence.cli;

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.occurrence.cli.common.GangliaConfiguration;
import org.gbif.occurrence.cli.common.ZooKeeperConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Objects;

public class ProcessorConfiguration {

  @ParametersDelegate
  @NotNull
  @Valid
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @ParametersDelegate
  @Valid
  @NotNull
  public ZooKeeperConfiguration zooKeeper = new ZooKeeperConfiguration();

  @ParametersDelegate
  @Valid
  @NotNull
  public GangliaConfiguration ganglia = new GangliaConfiguration();

  @Parameter(names = "--msg-pool-size")
  @Min(1)
  public int msgPoolSize = 10;

  @Parameter(names = "--hbase-pool-size")
  @Min(1)
  public int hbasePoolSize = 5;

  @Parameter(names = "--primary-queue-name")
  @NotNull
  public String primaryQueueName;

  @Parameter(names = "--secondary-queue-name")
  public String secondaryQueueName;

  @Parameter(names = "--occ-table")
  @NotNull
  public String occTable;

  @Parameter(names = "--occ-counter-table")
  @NotNull
  public String counterTable;

  @Parameter(names = "--occ-lookup-table")
  @NotNull
  public String lookupTable;

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("messaging", messaging).add("zooKeeper", zooKeeper).add("ganglia", ganglia)
      .add("msgPoolSize", msgPoolSize).add("hbasePoolSize", hbasePoolSize).add("queueName", primaryQueueName)
      .add("occTable", occTable).add("counterTable", counterTable).add("lookupTable", lookupTable).toString();
  }
}
