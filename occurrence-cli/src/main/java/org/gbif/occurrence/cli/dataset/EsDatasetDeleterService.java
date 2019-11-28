package org.gbif.occurrence.cli.dataset;

import org.gbif.common.messaging.MessageListener;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.http.HttpHost;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service that listens to {@link
 * org.gbif.common.messaging.api.messages.DeleteDatasetOccurrencesMessage} messages.
 */
public class EsDatasetDeleterService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(EsDatasetDeleterService.class);

  private final EsDatasetDeleterConfiguration config;
  private MessageListener listener;
  private RestHighLevelClient esClient;
  private FileSystem fs;

  public EsDatasetDeleterService(EsDatasetDeleterConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting pipelines-dataset-deleter service with params: {}", config);
    listener = new MessageListener(config.messaging.getConnectionParameters());
    esClient = createEsClient();
    fs = createFs();

    config.ganglia.start();

    listener.listen(
        config.queueName,
        config.poolSize,
        new EsDatasetDeleterCallback(esClient, fs, config));
  }

  @Override
  protected void shutDown() throws Exception {
    if (listener != null) {
      listener.close();
    }
    if (esClient != null) {
      esClient.close();
    }
    if (fs != null) {
      fs.close();
    }
  }

  private FileSystem createFs() throws IOException {
    Configuration cf = new Configuration();
    // check if the hdfs-site.xml is provided
    if (!Strings.isNullOrEmpty(config.hdfsSiteConfig)) {
      File hdfsSite = new File(config.hdfsSiteConfig);
      if (hdfsSite.exists() && hdfsSite.isFile()) {
        LOG.info("using hdfs-site.xml");
        cf.addResource(hdfsSite.toURI().toURL());
      } else {
        LOG.warn("hdfs-site.xml does not exist");
      }
    }

    return FileSystem.get(cf);
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
                    requestConfigBuilder
                        .setConnectTimeout(config.esConnectTimeout)
                        .setSocketTimeout(config.esSocketTimeout))
            .setMaxRetryTimeoutMillis(config.esSocketTimeout)
            .setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);

    return new RestHighLevelClient(builder);
  }
}
