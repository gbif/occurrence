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

import org.gbif.api.service.checklistbank.NameUsageMatchingService;
import org.gbif.occurrence.search.clb.NameUsageMatchingServiceClient;
import org.gbif.occurrence.search.es.EsConfig;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.http.HttpHost;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;

/** Occurrence search configuration. */
public class OccurrenceSearchConfiguration  {

  private static final String PREFIX = "occurrence.search.";
  private static final String ES_PREFIX = "es.";


  @ConfigurationProperties(prefix = "occurrence.search.es")
  @Bean
  public EsConfig esConfig() {
    return new EsConfig();
  }

  @Bean
  public RestHighLevelClient provideEsClient(EsConfig esConfig) {
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

    RestHighLevelClient highLevelClient = new RestHighLevelClient(builder);

    if (esConfig.getSniffInterval() > 0) {
      Sniffer sniffer = Sniffer.builder(highLevelClient.getLowLevelClient())
        .setSniffIntervalMillis(esConfig.getSniffInterval())
        .setSniffAfterFailureDelayMillis(esConfig.getSniffAfterFailureDelay())
        .build();
      sniffOnFailureListener.setSniffer(sniffer);

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        sniffer.close();
        try {
          highLevelClient.close();
        } catch (IOException e) {
          throw new IllegalStateException("Couldn't close ES client", e);
        }
      }));
    }


    return highLevelClient;
  }

  @Bean
  public NameUsageMatchingService nameUsageMatchingServiceClient(@Value("${api.url}") String apiUrl) {
    return new ClientBuilder()
        .withUrl(apiUrl)
        .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
        .withFormEncoder()
        .build(NameUsageMatchingServiceClient.class);
  }
}
