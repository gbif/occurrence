package org.gbif.occurrence.cli.index;

import org.gbif.common.search.solr.SolrServerType;
import org.gbif.common.search.solr.builders.CloudSolrServerBuilder;
import org.gbif.occurrence.search.writer.SolrOccurrenceWriter;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

/**
 * A base class for services that will insert/update occurrences in the Occurrence Index.
 */
class IndexUpdaterService extends AbstractIdleService {

  private final IndexingConfiguration configuration;
  private IndexMessageListener listener;

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

    SolrOccurrenceWriter solrOccurrenceWriter = new SolrOccurrenceWriter(buildSolrServer(configuration),
                                                                         configuration.commitWithinMs);
    listener = new IndexMessageListener(configuration.messaging.getConnectionParameters());
    listener.listen(configuration.queueName, configuration.poolSize, new IndexUpdaterCallback(solrOccurrenceWriter,
                    configuration.solrUpdateBatchSize, configuration.solrUpdateWithinMs));
  }

  /**
   * Creates a Solr server instance according to the parameters defined in the idxConfiguration object.
   */
  private static SolrClient buildSolrServer(IndexingConfiguration idxConfiguration) {
    if (Strings.isNullOrEmpty(idxConfiguration.solrServerType)
        || SolrServerType.HTTP.name().equalsIgnoreCase(idxConfiguration.solrServerType)) {
      return new HttpSolrClient(idxConfiguration.solrServer);
    } else if (SolrServerType.CLOUD.name().equalsIgnoreCase(idxConfiguration.solrServerType)) {
      CloudSolrServerBuilder cloudSolrServerBuilder = new CloudSolrServerBuilder();
      cloudSolrServerBuilder.withDefaultCollection(idxConfiguration.solrCollection).withZkHost(idxConfiguration.solrServer);
      return cloudSolrServerBuilder.build();
    } else {
      throw new IllegalArgumentException(String.format("Solr server type %s not supported",
        idxConfiguration.solrServerType));
    }
  }

}
