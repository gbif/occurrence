package org.gbif.occurrence.search.heatmap;

import org.gbif.common.search.inject.SolrModule;
import org.gbif.service.guice.PrivateServiceModule;

import java.util.Properties;

/**
 * Occurrence search guice module.
 */
public class OccurrenceHeatmapsModule extends PrivateServiceModule {

  private static final String PREFIX = "occurrence.search.";

  public OccurrenceHeatmapsModule(Properties properties) {
    super(PREFIX, properties);
  }

  @Override
  protected void configureService() {
    install(new SolrModule());
    bind(OccurrenceHeatmapsService.class);
    expose(OccurrenceHeatmapsService.class);
  }

}
