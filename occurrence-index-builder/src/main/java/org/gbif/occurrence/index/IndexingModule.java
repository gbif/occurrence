package org.gbif.occurrence.index;

import org.gbif.common.search.inject.SolrModule;
import org.gbif.service.guice.PrivateServiceModule;

import java.util.Properties;

import org.apache.solr.client.solrj.SolrServer;


/**
 * Guice module that bind the indexing classes.
 * This module depends on the registry client and checklist bank mybatis modules to be installed too
 * which is done in the {@link IndexingModule}.
 */
public class IndexingModule extends PrivateServiceModule {

  private static final String PREFIX = "occurrence.indexer.";

  public IndexingModule(Properties properties) {
    super(PREFIX, properties);
  }

  @Override
  protected void configureService() {
    install(new SolrModule());
    expose(SolrServer.class);
  }

}
