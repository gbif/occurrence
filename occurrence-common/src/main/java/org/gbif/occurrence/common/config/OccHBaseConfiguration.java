/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.common.config;

import java.util.StringJoiner;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;

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

  @Parameter(names = "--relationship-table")
  public String relationshipTable;

  @Parameter(names = "--relationship-salt")
  public int relationshipSalt = 10;

  /**
   * The zookeeper connection being used to create a lock provider
   */
  @Parameter(names = "--occ-zk-connection")
  @NotNull
  public String zkConnectionString;

  public int getHbasePoolSize() {
    return hbasePoolSize;
  }

  public void setHbasePoolSize(int hbasePoolSize) {
    this.hbasePoolSize = hbasePoolSize;
  }

  public String getFragmenterTable() {
    return fragmenterTable;
  }

  public void setFragmenterTable(String fragmenterTable) {
    this.fragmenterTable = fragmenterTable;
  }

  public int getFragmenterSalt() {
    return fragmenterSalt;
  }

  public void setFragmenterSalt(int fragmenterSalt) {
    this.fragmenterSalt = fragmenterSalt;
  }

  public String getZkConnectionString() {
    return zkConnectionString;
  }

  public void setZkConnectionString(String zkConnectionString) {
    this.zkConnectionString = zkConnectionString;
  }

  public String getRelationshipTable() {
    return relationshipTable;
  }

  public void setRelationshipTable(String relationshipTable) {
    this.relationshipTable = relationshipTable;
  }

  public int getRelationshipSalt() {
    return relationshipSalt;
  }

  public void setRelationshipSalt(int relationshipSalt) {
    this.relationshipSalt = relationshipSalt;
  }

  /**
   * Uses conventions to populate all table names based on the environment prefix. Only used in tests!
   * @param prefix environment prefix, e.g. prod or uat
   */
  public void setEnvironment(String prefix) {
    fragmenterTable = prefix + "_fragment";
  }


  @Override
  public String toString() {
    return new StringJoiner(", ", OccHBaseConfiguration.class.getSimpleName() + "[", "]")
      .add("hbasePoolSize=" + hbasePoolSize)
      .add("fragmenterTable='" + fragmenterTable + "'")
      .add("fragmenterSalt=" + fragmenterSalt)
      .add("relationshipTable='" + relationshipTable + "'")
      .add("relationshipSalt=" + relationshipSalt)
      .add("zkConnectionString='" + zkConnectionString + "'")
      .toString();
  }
}
