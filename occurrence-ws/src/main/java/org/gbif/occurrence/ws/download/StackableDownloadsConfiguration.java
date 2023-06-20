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
package org.gbif.occurrence.ws.download;

import org.gbif.stackable.ConfigUtils;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.kubernetes.client.util.KubeConfig;

/**
 * Spring configuration for K8 Stackable downloads.
 */
@Configuration
public class StackableDownloadsConfiguration {

  @ConfigurationProperties(prefix = "occurrence.download.watcher")
  @Bean
  public WatcherConfiguration watcherConfiguration() {
    return new WatcherConfiguration();
  }

  @Bean
  public KubeConfig kubeConfig(@Value("${stackable.kubeConfigFile}") String  kubeConfigFile) {
    return ConfigUtils.loadKubeConfig(kubeConfigFile);
  }

}
