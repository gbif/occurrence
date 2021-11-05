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
 * A configuration class which can be used to get all the details needed to create a connection to ZooKeeper needed by
 * the Curator Framework.
 */
@SuppressWarnings("PublicField")
public class ZooKeeperConfiguration {

  @Parameter(
    names = "--zk-connection-string",
    description = "The connection string to connect to ZooKeeper")
  @NotNull
  public String connectionString;

  @Parameter(
    names = "--zk-namespace",
    description = "The namespace in ZooKeeper under which all data lives")
  @NotNull
  public String namespace;

  @Parameter(
    names = "--zk-sleep-time",
    description = "Initial amount of time to wait between retries in ms")
  @Min(1)
  public int baseSleepTime = 1000;

  @Parameter(
    names = "--zk-max-retries",
    description = "Max number of times to retry")
  @Min(1)
  public int maxRetries = 10;

  @Override
  public String toString() {
    return new StringJoiner(", ", ZooKeeperConfiguration.class.getSimpleName() + "[", "]")
      .add("connectionString='" + connectionString + "'")
      .add("namespace='" + namespace + "'")
      .add("baseSleepTime=" + baseSleepTime)
      .add("maxRetries=" + maxRetries)
      .toString();
  }
}
