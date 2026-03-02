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
package org.gbif.occurrence.search.configuration;

import org.gbif.occurrence.search.es.EsConfig;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;

import org.apache.http.HttpHost;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.springframework.beans.factory.annotation.Value;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;

/** Occurrence search configuration. */
public class OccurrenceSearchConfiguration  {

  @ConfigurationProperties(prefix = "occurrence.search.es")
  @Bean
  public EsConfig esConfig() {
    return new EsConfig();
  }

  @Bean
  public ElasticsearchClient provideEsClient(EsConfig esConfig) {
    HttpHost[] hosts = new HttpHost[esConfig.getHosts().length];
    int i = 0;
    for (String host : esConfig.getHosts()) {
      try {
        URL url = new URL(host);
        hosts[i] = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
        i++;
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(e.getMessage(), e);
      }
    }

    RestClientBuilder builder =
        RestClient.builder(hosts)
            .setRequestConfigCallback(
                requestConfigBuilder ->
                    requestConfigBuilder
                        .setConnectTimeout(esConfig.getConnectTimeout())
                        .setSocketTimeout(esConfig.getSocketTimeout()))
            .setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);

    RestClient restClient = builder.build();
    RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
    ElasticsearchClient elasticsearchClient = new ElasticsearchClient(transport);

    Runtime.getRuntime().addShutdownHook(
        new Thread(
          elasticsearchClient::shutdown));

    return elasticsearchClient;
  }

  @Bean
  public NameUsageMatchingService nameUsageMatchingService(@Value("${nameUsageMatchingService.ws.url}") String apiUrl) {
    return new ClientBuilder()
        .withUrl(apiUrl)
        .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
        .withFormEncoder()
        .withExponentialBackoffRetry(Duration.ofMillis(250), 1.0, 3)
        .build(NameUsageMatchingService.class);
  }
}
