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
package org.gbif.occurrence.ws.config;

import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.persistence.configuration.OccurrencePersistenceConfiguration;
import org.gbif.occurrence.query.TitleLookupService;
import org.gbif.occurrence.query.TitleLookupServiceFactory;
import org.gbif.occurrence.search.configuration.OccurrenceSearchConfiguration;
import org.gbif.occurrence.search.es.OccurrenceBaseEsFieldMapper;
import org.gbif.occurrence.search.es.OccurrenceEsField;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OccurrenceWsConfiguration {



  @Bean
  public TitleLookupService titleLookupService(@Value("${api.url}") String apiUrl) {
    return TitleLookupServiceFactory.getInstance(apiUrl);
  }

  @Bean
  public OccurrenceDownloadService occurrenceDownloadService(
      @Value("${api.url}") String apiUrl,
      @Value("${occurrence.download.ws.username}") String downloadUsername,
      @Value("${occurrence.download.ws.password}") String downloadUserPassword) {
    return new ClientBuilder()
        .withUrl(apiUrl)
        .withCredentials(downloadUsername, downloadUserPassword)
        .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
        .withFormEncoder()
        .withExponentialBackoffRetry(Duration.ofSeconds(1), 1.0, 5)
        .build(OccurrenceDownloadClient.class);
  }

  @Configuration
  public static class OccurrenceSearchConfigurationWs extends OccurrenceSearchConfiguration {

    @Value("${defaultChecklistKey}")
    protected String defaultChecklistKey;

    @Bean
    public OccurrenceBaseEsFieldMapper esFieldMapper() {
      return OccurrenceEsField.buildFieldMapper(defaultChecklistKey);
    }
  }

  @Configuration
  public static class OccurrencePersistenceConfigurationWs
      extends OccurrencePersistenceConfiguration {}
}
