package org.gbif.occurrence.cli.registry.service.sync;

import javax.validation.constraints.NotNull;

/**
 * Configuration related to HBase
 */
public class HBaseConfiguration {

  @NotNull
  public String zkConnectionString;

  @NotNull
  public String occurrenceTable;

}
