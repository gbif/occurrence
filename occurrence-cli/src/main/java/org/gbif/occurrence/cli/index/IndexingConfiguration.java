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

  @Parameter(names = "--solr-server")
  @NotNull
  public String solrServer;

  @Parameter(names = "--solr-server-type")
  @NotNull
  public String solrServerType;

  @Parameter(names = "--solr-collection")
  @NotNull
  public String solrCollection;


  @Parameter(names = "--solr-update-batch-size")
  @NotNull
  public int solrUpdateBatchSize = 1000;

  @Parameter(names = "--solr-update-within")
  @NotNull
  public long solrUpdateWithinMs = 120000;

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("messaging", messaging).add("ganglia", ganglia).add("poolSize", poolSize)
      .add("commitWithinMs", commitWithinMs).add("queueName", queueName).add("solrServer", solrServer)
      .add("solrServerType", solrServerType).add("solrCollection", solrCollection)
      .add("solrUpdateBatchSize", solrUpdateBatchSize).add("solrUpdateWithinMs", solrUpdateWithinMs)
      .toString();
  }
}
