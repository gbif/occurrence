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

  @Parameter(names = "--fragmenter-table")
  public String fragmenterTable;

  @Parameter(names = "--fragmenter-salt")
  public int fragmenterSalt = 100;

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
    fragmenterTable = prefix + "_fragment";
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("hbasePoolSize", hbasePoolSize)
      .add("fragmenterTable", fragmenterTable)
      .add("zkConnectionString", zkConnectionString)
      .toString();
  }

}
