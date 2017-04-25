package org.gbif.occurrence.search.heatmap;

import org.gbif.common.search.solr.SolrModule;
import org.gbif.common.search.solr.SolrConfig;
import org.gbif.service.guice.PrivateServiceModule;

import java.util.Properties;

/**
 * Occurrence search guice module.
 */
public class OccurrenceHeatmapsModule extends PrivateServiceModule {

  private static final String PREFIX = "occurrence.search.";
  private static final String SOLR_PREFIX = PREFIX + "solr.";
  private final SolrConfig solrConfig;

  public OccurrenceHeatmapsModule(Properties properties) {
    super(PREFIX, properties);
    solrConfig = SolrConfig.fromProperties(properties, SOLR_PREFIX);
  }

  @Override
  protected void configureService() {
    install(new SolrModule(solrConfig));
    bind(OccurrenceHeatmapsService.class);
    expose(OccurrenceHeatmapsService.class);
  }

}
