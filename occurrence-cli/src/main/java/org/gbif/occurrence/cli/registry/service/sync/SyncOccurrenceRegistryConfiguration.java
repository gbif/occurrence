package org.gbif.occurrence.cli.registry.service.sync;

import org.gbif.common.messaging.config.MessagingConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

/**
 *
 */
public class SyncOccurrenceRegistryConfiguration {

  @ParametersDelegate
  @Valid
  @NotNull
  public MessagingConfiguration messaging = new MessagingConfiguration();

  @ParametersDelegate
  @Valid
  @NotNull
  public HBaseConfiguration hbase = new HBaseConfiguration();

  @ParametersDelegate
  @Valid
  @NotNull
  public MapReduceConfiguration mapReduce = new MapReduceConfiguration();

  @Parameter(names = "--registry-ws-url")
  @NotNull
  public String registryWsUrl;

  @Parameter(names = "--dataset-key")
  public String datasetKey;

  @Parameter(names = "--since")
  public Long since;


  public class HBaseConfiguration {

    @NotNull
    public String occurrenceTable;

    public String timeoutMs = "600000";
  }

  public static class MapReduceConfiguration {
    @NotNull
    public String mapMemoryMb;

    @NotNull
    public String mapJavaOpts;

    @NotNull
    public String queueName;
  }

}
