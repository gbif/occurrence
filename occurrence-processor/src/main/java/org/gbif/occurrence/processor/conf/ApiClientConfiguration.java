package org.gbif.occurrence.processor.conf;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import com.beust.jcommander.Parameter;
import com.google.common.base.Objects;

public class ApiClientConfiguration {

  /**
   * The base URL to the GBIF API.
   */
  @Parameter(names = "--api-url")
  @NotNull
  public String url;

  /**
   * http timeout in milliseconds.
   */
  @Parameter(names = "--api-timeout")
  @Min(10)
  public int timeout = 3000;

  /**
   * maximum allowed parallel http connections.
   */
  @Parameter(names = "--max-connections")
  @Min(10)
  public int maxConnections = 100;


  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("url", url)
      .add("timeout", timeout)
      .add("maxConnections", maxConnections)
      .toString();
  }
}
