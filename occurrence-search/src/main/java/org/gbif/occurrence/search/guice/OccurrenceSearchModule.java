package org.gbif.occurrence.search.guice;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.common.search.solr.SolrConfig;
import org.gbif.common.search.solr.SolrModule;
import org.gbif.occurrence.search.es.OccurrenceSearchESImpl;
import org.gbif.occurrence.search.es.EsConfig;
import org.gbif.service.guice.PrivateServiceModule;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

/** Occurrence search guice module. */
public class OccurrenceSearchModule extends PrivateServiceModule {

  private static final String PREFIX = "occurrence.search.";
  private static final String SOLR_PREFIX = PREFIX + "solr.";
  private static final String ES_PREFIX = "es.";
  private final SolrConfig solrConfig;
  private final EsConfig esConfig;

  public OccurrenceSearchModule(Properties properties) {
    super(PREFIX, properties);
    solrConfig = SolrConfig.fromProperties(properties, SOLR_PREFIX);
    esConfig = EsConfig.fromProperties(getProperties(), ES_PREFIX);
  }

  @Override
  protected void configureService() {
    install(new SolrModule(solrConfig));
    bind(OccurrenceSearchService.class).to(OccurrenceSearchESImpl.class);
    expose(OccurrenceSearchService.class);
  }

  @Provides
  @Singleton
  private RestClient provideEsClient() {
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
    return RestClient.builder(hosts).build();
  }
}
