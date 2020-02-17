package org.gbif.occurrence.cli.index;

import java.net.MalformedURLException;
import java.net.URL;

import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * A base class for services that will insert/update occurrences in the Occurrence Index.
 */
// TODO: remove this. It is not needed in ES.
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
  }

  private RestHighLevelClient provideEsClient() {
    String[] confHosts = configuration.esHosts.split(",");
    HttpHost[] hosts = new HttpHost[confHosts.length];
    int i = 0;
    for (String host : confHosts) {
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
          requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(6000).setSocketTimeout(90000))
        .setMaxRetryTimeoutMillis(90000);

    return new RestHighLevelClient(builder);
  }


}
