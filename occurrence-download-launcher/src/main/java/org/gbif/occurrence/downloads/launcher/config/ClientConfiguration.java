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

import org.gbif.occurrence.downloads.launcher.pojo.RegistryConfiguration;
import org.gbif.occurrence.downloads.launcher.pojo.StackableConfiguration;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.gbif.stackable.ConfigUtils;
import org.gbif.stackable.K8StackableSparkController;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Configuration for the clients used by the launcher. */
@Configuration
public class ClientConfiguration {

  @Bean
  public K8StackableSparkController k8StackableSparkController(
      StackableConfiguration stackableConfiguration) {
    return K8StackableSparkController.builder()
        .kubeConfig(ConfigUtils.loadKubeConfig(stackableConfiguration.kubeConfigFile))
        .build();
  }

  /**
   * Provides an OccurrenceDownloadClient.
   *
   * @param configuration the registry configuration
   * @return an OccurrenceDownloadClient
   */
  @Bean
  public OccurrenceDownloadClient occurrenceDownloadClient(RegistryConfiguration configuration) {
    return new ClientBuilder()
        .withUrl(configuration.getApiUrl())
        .withCredentials(configuration.getUserName(), configuration.getPassword())
        .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
        .withFormEncoder()
        .build(OccurrenceDownloadClient.class);
  }
}
