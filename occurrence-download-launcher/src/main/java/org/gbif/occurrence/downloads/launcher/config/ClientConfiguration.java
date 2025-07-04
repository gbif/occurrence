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

import java.time.Duration;
import org.gbif.occurrence.downloads.launcher.pojo.RegistryConfiguration;
import org.gbif.registry.ws.client.EventDownloadClient;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Configuration for the clients used by the launcher. */
@Configuration
public class ClientConfiguration {

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
        // This will give up to 40 tries, from 2 to 75 seconds apart, over at most 13 minutes (approx)
        .withExponentialBackoffRetry(Duration.ofSeconds(2), 1.1, 40)
        .build(OccurrenceDownloadClient.class);
  }

  /**
   * Provides an EventDownloadClient.
   *
   * @param configuration the registry configuration
   * @return an EventDownloadClient
   */
  @Bean
  public EventDownloadClient eventDownloadClient(RegistryConfiguration configuration) {
    return new ClientBuilder()
      .withUrl(configuration.getApiUrl())
      .withCredentials(configuration.getUserName(), configuration.getPassword())
      .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
      .withFormEncoder()
      // This will give up to 40 tries, from 2 to 75 seconds apart, over at most 13 minutes (approx)
      .withExponentialBackoffRetry(Duration.ofSeconds(2), 1.1, 40)
      .build(EventDownloadClient.class);
  }
}
