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
package org.gbif.occurrence.ws.it;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.gbif.occurrence.search.es.EsConfig;
import org.gbif.occurrence.test.mocks.ConceptClientMock;
import org.gbif.occurrence.test.servers.EsManageServer;
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.gbif.vocabulary.client.ConceptClient;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.Resource;

@TestConfiguration
public class OccurrenceSearchWsItConfiguration {

  @Bean
  public EsManageServer esManageServer(
      @Value("classpath:elasticsearch/es-settings.json") Resource settings,
      @Value("classpath:elasticsearch/es-occurrence-schema.json") Resource mappings) {
    return EsManageServer.builder()
        .indexName("occurrence")
        .keyField("gbifId")
        .settingsFile(settings)
        .mappingFile(mappings)
        .build();
  }

  @Bean
  @Primary
  @ConfigurationProperties(prefix = "occurrence.search.es")
  public EsConfig esConfig(EsManageServer esManageServer) {
    EsConfig esConfig = new EsConfig();
    esConfig.setHosts(new String[] {esManageServer.getServerAddress()});
    esConfig.setIndex(esManageServer.getIndexName());
    return esConfig;
  }

  @Bean
  @Primary
  public ElasticsearchClient elasticsearchClient(EsManageServer esManageServer) {
    return esManageServer.getRestClient();
  }

  @Bean
  @Primary
  public NameUsageMatchingService nameUsageMatchingService() {
    return Mockito.mock(NameUsageMatchingService.class);
  }

  @Bean
  @Primary
  public ConceptClient conceptClient() {
    return new ConceptClientMock();
  }
}
