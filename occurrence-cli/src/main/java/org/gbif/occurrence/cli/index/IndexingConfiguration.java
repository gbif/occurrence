package org.gbif.occurrence.cli.index;

import org.gbif.common.messaging.config.MessagingConfiguration;
import org.gbif.occurrence.cli.common.GangliaConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Objects;

/**
 * Configuration for indexing jobs that process updates, deletions and insertions.
 */
class IndexingConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @ParametersDelegate
  @Valid
  @NotNull
  public GangliaConfiguration ganglia = new GangliaConfiguration();

  @Parameter(names = "--pool-size")
  @Min(1)
  public int poolSize = 5;

  @Parameter(names = "--msg-pool-size")
  @Min(1)
  public int msgPoolSize = 50;

  @Parameter(names = "--commit-within")
  @NotNull
  public int commitWithinMs = 120000;

  @Parameter(names = "--queue-name")
  @NotNull
  public String queueName;

  @Parameter(names = "--es-hosts")
  @NotNull
  public String esHosts;

  @Parameter(names = "--es-index")
  @NotNull
  public String esIndex;


  @Parameter(names = "--es-update-batch-size")
  @NotNull
  public int updateBatchSize = 1000;


  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("messaging", messaging).add("ganglia", ganglia)
      .add("poolSize", poolSize).add("msgPoolSize", msgPoolSize)
      .add("commitWithinMs", commitWithinMs).add("queueName", queueName).add("solrServer", solrServer)
      .add("esHosts", esHosts).add("esIndex", esIndex)
      .add("updateBatchSize", updateBatchSize)
      .toString();
  }
}
