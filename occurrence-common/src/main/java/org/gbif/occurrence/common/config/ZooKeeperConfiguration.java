package org.gbif.occurrence.common.config;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.google.common.base.Objects;

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
    return Objects.toStringHelper(this).add("connectionString", connectionString).add("namespace", namespace)
      .add("baseSleepTime", baseSleepTime).add("maxRetries", maxRetries).toString();

  }
}
