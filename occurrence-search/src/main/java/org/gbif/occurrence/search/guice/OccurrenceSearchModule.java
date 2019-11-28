package org.gbif.occurrence.search.guice;

import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.occurrence.search.OccurrenceGetByKey;
import org.gbif.occurrence.search.es.EsConfig;
import org.gbif.occurrence.search.es.OccurrenceSearchEsImpl;
import org.gbif.service.guice.PrivateServiceModule;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.http.HttpHost;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;

/** Occurrence search guice module. */
public class OccurrenceSearchModule extends PrivateServiceModule {

  private static final String PREFIX = "occurrence.search.";
  private static final String ES_PREFIX = "es.";
  private final EsConfig esConfig;

  public OccurrenceSearchModule(Properties properties) {
    super(PREFIX, properties);
    esConfig = EsConfig.fromProperties(getProperties(), ES_PREFIX);
  }

  @Override
  protected void configureService() {
    bind(OccurrenceSearchService.class).to(OccurrenceSearchEsImpl.class);
    bind(OccurrenceGetByKey.class).to(OccurrenceSearchEsImpl.class);
    expose(OccurrenceSearchService.class);
    expose(OccurrenceGetByKey.class);
  }

  @Provides
  @Singleton
  private RestHighLevelClient provideEsClient() {
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
            .setMaxRetryTimeoutMillis(esConfig.getSocketTimeout())
            .setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS)
            .setFailureListener(sniffOnFailureListener);

    RestHighLevelClient highLevelClient = new RestHighLevelClient(builder);

    Sniffer sniffer =
        Sniffer.builder(highLevelClient.getLowLevelClient())
            .setSniffIntervalMillis(esConfig.getSniffInterval())
            .setSniffAfterFailureDelayMillis(esConfig.getSniffAfterFailureDelay())
            .build();
    sniffOnFailureListener.setSniffer(sniffer);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  sniffer.close();
                  try {
                    highLevelClient.close();
                  } catch (IOException e) {
                    throw new IllegalStateException("Couldn't close ES client", e);
                  }
                }));

    return highLevelClient;
  }
}
