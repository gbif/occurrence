package org.gbif.occurrence.cli.dataset;

import org.gbif.common.messaging.MessageListener;

import java.net.MalformedURLException;
import java.net.URL;

import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service that listens to {@link
 * org.gbif.common.messaging.api.messages.DeleteDatasetOccurrencesMessage} messages.
 */
public class PipelinesDatasetDeleterService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(PipelinesDatasetDeleterService.class);

  private final PipelinesDatasetDeleterConfiguration config;
  private MessageListener listener;
  private RestHighLevelClient esClient;

  public PipelinesDatasetDeleterService(PipelinesDatasetDeleterConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting pipelines-dataset-deleter service with params: {}", config);
    listener = new MessageListener(config.messaging.getConnectionParameters());
    esClient = createEsClient();

    config.ganglia.start();

    listener.listen(
        config.queueName,
        config.poolSize,
        new PipelinesDatasetDeleterCallback(esClient, config.esIndex));
  }

  @Override
  protected void shutDown() throws Exception {
    if (listener != null) {
      listener.close();
    }
    if (esClient != null) {
      esClient.close();
    }
  }

  private RestHighLevelClient createEsClient() {
    HttpHost[] hosts = new HttpHost[config.esHosts.length];
    int i = 0;
    for (String host : config.esHosts) {
      try {
        URL url = new URL(host);
        hosts[i] = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
        i++;
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(e.getMessage(), e);
      }
    }

    RestClientBuilder builder =
        RestClient.builder(hosts)
            .setRequestConfigCallback(
                requestConfigBuilder ->
                    requestConfigBuilder.setConnectTimeout(6000).setSocketTimeout(90000))
            .setMaxRetryTimeoutMillis(90000);

    return new RestHighLevelClient(builder);
  }
}
