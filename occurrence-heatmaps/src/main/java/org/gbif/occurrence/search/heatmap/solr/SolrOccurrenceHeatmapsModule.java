package org.gbif.occurrence.search.heatmap.solr;

import org.gbif.common.search.solr.SolrModule;
import org.gbif.common.search.solr.SolrConfig;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapService;
import org.gbif.service.guice.PrivateServiceModule;

import java.util.Properties;

/**
 * Occurrence search guice module.
 */
public class SolrOccurrenceHeatmapsModule extends PrivateServiceModule {

  private static final String PREFIX = "occurrence.search.";
  private static final String SOLR_PREFIX = PREFIX + "solr.";
  private final SolrConfig solrConfig;

  public SolrOccurrenceHeatmapsModule(Properties properties) {
    super(PREFIX, properties);
    solrConfig = SolrConfig.fromProperties(properties, SOLR_PREFIX);
  }

  @Override
  protected void configureService() {
    install(new SolrModule(solrConfig));
    bind(OccurrenceHeatmapService.class).to(SolrOccurrenceHeatmapsService.class);
    expose(OccurrenceHeatmapService.class);
  }

}
