package org.gbif.occurrence.search.guice;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.common.search.solr.SolrConfig;
import org.gbif.common.search.solr.SolrModule;
import org.gbif.occurrence.search.OccurrenceSearchESImpl;
import org.gbif.occurrence.search.es.EsConfig;
import org.gbif.service.guice.PrivateServiceModule;

import java.util.Properties;

/**
 * Occurrence search guice module.
 */
public class OccurrenceSearchModule extends PrivateServiceModule {

  private static final String PREFIX = "occurrence.search.";
  private static final String SOLR_PREFIX = PREFIX + "solr.";
  private static final String ES_PREFIX = "es.";
  private final SolrConfig solrConfig;

  public OccurrenceSearchModule(Properties properties) {
    super(PREFIX, properties);
    solrConfig = SolrConfig.fromProperties(properties, SOLR_PREFIX);
  }

  @Override
  protected void configureService() {
    install(new SolrModule(solrConfig));
    bind(OccurrenceSearchService.class).to(OccurrenceSearchESImpl.class);
    expose(OccurrenceSearchService.class);
    expose(EsConfig.class);
  }

  @Provides
  @Singleton
  private EsConfig provideEsConfig() {
    return EsConfig.fromProperties(getProperties(), ES_PREFIX);
  }

}
