package org.gbif.occurrence.search.guice;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.occurrence.search.OccurrenceGetByKey;
import org.gbif.occurrence.search.es.EsConfig;
import org.gbif.occurrence.search.es.OccurrenceSearchEsImpl;
import org.gbif.service.guice.PrivateServiceModule;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

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

    RestClientBuilder builder =
        RestClient.builder(hosts)
            .setRequestConfigCallback(
              requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(6000).setSocketTimeout(90000))
            .setMaxRetryTimeoutMillis(90000);

    return new RestHighLevelClient(builder);
  }
}
