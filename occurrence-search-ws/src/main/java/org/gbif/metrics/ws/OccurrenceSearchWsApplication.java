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
package org.gbif.metrics.ws;

import org.gbif.vocabulary.client.ConceptClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchRestClientAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication(exclude = {ElasticsearchRestClientAutoConfiguration.class})
@EnableConfigurationProperties
@ComponentScan(
    basePackages = {
      "org.gbif.ws.server.advice",
      "org.gbif.ws.server.mapper",
      "org.gbif.metrics.ws",
      "org.gbif.occurrence.search"
    })
public class OccurrenceSearchWsApplication {

  public static void main(String[] args) {
    SpringApplication.run(OccurrenceSearchWsApplication.class, args);
  }

  @Bean
  public ConceptClient conceptClient(@Value("${api.url}") String apiUrl) {
    return new ClientBuilder()
        .withUrl(apiUrl)
        .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
        .withExponentialBackoffRetry(Duration.ofSeconds(1), 1.0, 3)
        .build(ConceptClient.class);
  }
}
