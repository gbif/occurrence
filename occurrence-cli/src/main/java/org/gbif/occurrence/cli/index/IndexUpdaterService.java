package org.gbif.occurrence.cli.index;

import org.gbif.common.messaging.MessageListener;
import org.gbif.common.search.solr.SolrServerType;
import org.gbif.common.search.solr.builders.CloudSolrServerBuilder;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

/**
 * A base class for services that will insert/update occurrences in the Occurrence Index.
 */
class IndexUpdaterService extends AbstractIdleService {

  private final IndexingConfiguration configuration;
  private MessageListener listener;

  protected IndexUpdaterService(IndexingConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  protected void shutDown() throws Exception {
    if (listener != null) {
      listener.close();
    }
  }

  @Override
  protected void startUp() throws Exception {
    configuration.ganglia.start();

    SolrOccurrenceWriter solrOccurrenceWriter =
      new SolrOccurrenceWriter(buildSolrServer(configuration), configuration.commitWithinMs);
    listener = new MessageListener(configuration.messaging.getConnectionParameters());
    listener.listen(configuration.queueName, configuration.poolSize, new IndexUpdaterCallback(solrOccurrenceWriter));
  }

  /**
   * Creates a Solr server instance according to the parameters defined in the configuration object.
   */
  private SolrServer buildSolrServer(IndexingConfiguration configuration) {
    if (Strings.isNullOrEmpty(configuration.solrServerType)
      || SolrServerType.HTTP.name().equalsIgnoreCase(configuration.solrServerType)) {
      return new HttpSolrServer(configuration.solrServer);
    } else if (SolrServerType.CLOUD.name().equalsIgnoreCase(configuration.solrServerType)) {
      CloudSolrServerBuilder cloudSolrServerBuilder = new CloudSolrServerBuilder();
      cloudSolrServerBuilder.withDefaultCollection(configuration.solrCollection).withZkHost(configuration.solrServer);
      return cloudSolrServerBuilder.build();
    } else {
      throw new IllegalArgumentException(String.format("Solr server type %s not supported",
        configuration.solrServerType));
    }
  }
}
