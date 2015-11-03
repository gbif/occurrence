package org.gbif.occurrence.common.config;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.google.common.base.Objects;

/**
 * Configs needed to connect to the occurrence HBase db.
 */
public class OccHBaseConfiguration {

  @Parameter(names = "--hbase-pool-size")
  @Min(1)
  public int hbasePoolSize = 5;

  @Parameter(names = "--occ-table")
  @NotNull
  public String occTable;

  @Parameter(names = "--occ-counter-table")
  @NotNull
  public String counterTable;

  @Parameter(names = "--occ-lookup-table")
  @NotNull
  public String lookupTable;

  @Parameter(names = "--hbase-conf", description = "Specify the location of the hbase-site.xml file")
  @NotNull
  public String hbaseConfig;

  /**
   * The zookeeper connection being used to create a lock provider
   */
  @Parameter(names = "--occ-zk-connection")
  @NotNull
  public String zkConnectionString;

  /**
   * Uses conventions to populate all table names based on the environment prefix. Only used in tests!
   * @param prefix environment prefix, e.g. prod or uat
   */
  public void setEnvironment(String prefix) {
    occTable = prefix + "_occurrence";
    counterTable = prefix + "_occurrence_counter";
    lookupTable = prefix + "_occurrence_lookup";
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("hbasePoolSize", hbasePoolSize)
      .add("occTable", occTable)
      .add("counterTable", counterTable)
      .add("lookupTable", lookupTable)
      .add("zkConnectionString", zkConnectionString)
      .add("hbaseConfig", hbaseConfig)
      .toString();
  }

}
