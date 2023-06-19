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
package org.gbif.occurrence.downloads.launcher.pojo;

import com.beust.jcommander.Parameter;

import lombok.Data;

@Data
public class DistributedConfiguration {

  @Parameter(names = "--yarn-queue")
  public String yarnQueue;

  @Parameter(names = "--deploy-mode")
  public String deployMode;

  @Parameter(names = "--distributed-main-class")
  public String mainClass;

  @Parameter(names = "--distributed-jar-path")
  public String jarPath;

  @Parameter(names = "--driver-java-options")
  public String driverJavaOptions;

  @Parameter(names = "--extra-class-path")
  public String extraClassPath;

  @Parameter(names = "--metrics-properties-path")
  public String metricsPropertiesPath;

  @Parameter(names = "--other-user")
  public String otherUser;
}
