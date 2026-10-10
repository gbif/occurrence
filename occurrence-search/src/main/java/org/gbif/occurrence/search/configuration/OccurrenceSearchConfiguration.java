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
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.http.HttpHost;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

import com.fasterxml.jackson.databind.ObjectMapper;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import feign.Contract;
import feign.Feign;
import feign.Retryer;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;

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

    SniffOnFailureListener sniffOnFailureListener =
      new SniffOnFailureListener();

    RestClientBuilder builder =
        RestClient.builder(hosts)
            .setRequestConfigCallback(
                requestConfigBuilder ->
                    requestConfigBuilder
                        .setConnectTimeout(esConfig.getConnectTimeout())
                        .setSocketTimeout(esConfig.getSocketTimeout()))
            .setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);


    if (esConfig.getSniffInterval() > 0) {
      builder.setFailureListener(sniffOnFailureListener);
    }

    RestClient restClient = builder.build();
    ElasticsearchTransport transport =
        new RestClientTransport(restClient, new JacksonJsonpMapper());
    ElasticsearchClient esClient = new ElasticsearchClient(transport);

    Sniffer sniffer = null;
    if (esConfig.getSniffInterval() > 0) {
      sniffer = Sniffer.builder(restClient)
        .setSniffIntervalMillis(esConfig.getSniffInterval())
        .setSniffAfterFailureDelayMillis(esConfig.getSniffAfterFailureDelay())
        .build();
      sniffOnFailureListener.setSniffer(sniffer);
    }

    Sniffer finalSniffer = sniffer;
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      if (finalSniffer != null) {
        finalSniffer.close();
      }
      try {
        transport.close();
      } catch (IOException e) {
        throw new IllegalStateException("Couldn't close ES client", e);
      }
    }));

    return esClient;
  }

  /**
   * The client is annotated with the Feign annotations since kvs 3.1, which the Spring MVC contract
   * of the GBIF {@code ClientBuilder} doesn't read, so it's built with the default Feign contract.
   */
  @Bean
  public NameUsageMatchingService nameUsageMatchingService(@Value("${nameUsageMatchingService.ws.url}") String apiUrl) {
    ObjectMapper objectMapper = JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport();
    return Feign.builder()
        .contract(new Contract.Default())
        .encoder(new JacksonEncoder(objectMapper))
        .decoder(new JacksonDecoder(objectMapper))
        .retryer(new Retryer.Default(250, 1000, 3))
        .dismiss404()
        .target(NameUsageMatchingService.class, apiUrl);
  }
}
