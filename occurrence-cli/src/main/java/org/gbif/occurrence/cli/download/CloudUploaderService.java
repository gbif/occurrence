
package org.gbif.occurrence.cli.download;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.common.messaging.DefaultMessageRegistry;
import org.gbif.common.messaging.MessageListener;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.gbif.ws.client.ClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.io.File;
import java.io.IOException;

/**
 * Service that listens to {@link
 * org.gbif.common.messaging.api.messages.TransferDownloadToAzureMessage} messages.
 */
public class CloudUploaderService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(CloudUploaderService.class);

  private final CloudUploaderConfiguration config;
  private MessageListener listener;
  private FileSystem fs;

  public CloudUploaderService(CloudUploaderConfiguration config) {
    this.config = config;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting cloud-uploader-service service with params: {}", config);
    listener = new MessageListener(config.messaging.getConnectionParameters(), new DefaultMessageRegistry(), new ObjectMapper(), 1);

    OccurrenceDownloadService downloadService = occurrenceDownloadService(config.wsUrl);

    fs = createFs();

    config.ganglia.start();

    listener.listen(
        config.queueName,
        config.poolSize,
        new CloudUploaderCallback(downloadService, fs, config));
  }

  @Override
  protected void shutDown() throws Exception {
    if (listener != null) {
      listener.close();
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

  //  @Bean
  public OccurrenceDownloadService occurrenceDownloadService(@Value("${api.url}") String apiUrl) {
    ClientFactory clientFactory = new ClientFactory(apiUrl);
    return clientFactory.newInstance(OccurrenceDownloadClient.class);
  }

}
