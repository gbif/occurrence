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
package org.gbif.occurrence.download.stackable.config;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.yaml.snakeyaml.Yaml;

import lombok.SneakyThrows;

public class LauncherConfiguration {

  public DistributedConfiguration distributed;

  public StackableConfiguration stackable;

  public SparkStaticConfiguration spark;

  @SneakyThrows
  public static LauncherConfiguration fromYaml(String fileName) {
    Yaml yaml = new Yaml();
    return yaml.loadAs(Files.newBufferedReader(Paths.get(fileName)), LauncherConfiguration.class);
  }

}
