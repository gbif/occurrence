package org.gbif.occurrencestore.search.guice;

import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.common.search.inject.SolrModule;
import org.gbif.occurrencestore.search.OccurrenceSearchImpl;
import org.gbif.service.guice.PrivateServiceModule;

import java.util.Properties;

/**
 * Occurrence search guice module.
 */
public class OccurrenceSearchModule extends PrivateServiceModule {

  private static final String PREFIX = "occurrencestore.search.";

  public OccurrenceSearchModule(Properties properties) {
    super(PREFIX, properties);
  }

  @Override
  protected void configureService() {
    install(new SolrModule());
    bind(OccurrenceSearchService.class).to(OccurrenceSearchImpl.class);
    expose(OccurrenceSearchService.class);
  }

}
