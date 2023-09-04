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
