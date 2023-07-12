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
package org.gbif.occurrence.downloads.launcher.config;

import org.gbif.occurrence.downloads.launcher.pojo.DistributedConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.DownloadServiceConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.RegistryConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.SparkStaticConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.StackableConfiguration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Configuration for the services used by the launcher. */
@Configuration
public class YamlConfiguration {

  @ConfigurationProperties(prefix = "downloads")
  @Bean
  public DownloadServiceConfiguration downloadServiceConfiguration() {
    return new DownloadServiceConfiguration();
  }

  @ConfigurationProperties(prefix = "registry")
  @Bean
  public RegistryConfiguration registryConfiguration() {
    return new RegistryConfiguration();
  }

  @ConfigurationProperties(prefix = "distributed")
  @Bean
  public DistributedConfiguration distributedConfiguration() {
    return new DistributedConfiguration();
  }

  @ConfigurationProperties(prefix = "spark")
  @Bean
  public SparkStaticConfiguration sparkConfiguration() {
    return new SparkStaticConfiguration();
  }

  @ConfigurationProperties(prefix = "stackable")
  @Bean
  public StackableConfiguration stackableConfiguration() {
    return new StackableConfiguration();
  }
}
