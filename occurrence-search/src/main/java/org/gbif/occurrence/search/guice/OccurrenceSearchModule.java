package org.gbif.occurrence.search.guice;

import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.common.search.solr.SolrConfig;
import org.gbif.common.search.solr.SolrModule;
import org.gbif.occurrence.search.OccurrenceSearchImpl;
import org.gbif.service.guice.PrivateServiceModule;

import java.util.Properties;

/**
 * Occurrence search guice module.
 */
public class OccurrenceSearchModule extends PrivateServiceModule {

  private static final String PREFIX = "occurrence.search.";
  private static final String SOLR_PREFIX = PREFIX + "solr.";
  private final SolrConfig solrConfig;

  public OccurrenceSearchModule(Properties properties) {
    super(PREFIX, properties);
    solrConfig = SolrConfig.fromProperties(properties, SOLR_PREFIX);
  }

  @Override
  protected void configureService() {
    install(new SolrModule(solrConfig));
    bind(OccurrenceSearchService.class).to(OccurrenceSearchImpl.class);
    expose(OccurrenceSearchService.class);
  }

}
