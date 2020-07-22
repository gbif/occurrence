package org.gbif.occurrence.download.util;

import org.gbif.api.service.registry.DatasetOccurrenceDownloadUsageService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.DatasetOccurrenceDownloadUsageClient;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.gbif.ws.client.ClientBuilder;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

/**
 * Utility class to create registry web service clients.
 */
public class RegistryClientUtil {

  private static final Duration BACKOFF_INITIAL_INTERVAL = Duration.ofSeconds(4);
  private static final double BACKOFF_MULTIPLIER = 2d;
  private static final int BACKOFF_MAX_ATTEMPTS = 8;

  private final ClientBuilder clientBuilder = new ClientBuilder();

  public RegistryClientUtil(String userName, String password, String apiUrl) {
    clientBuilder
      .withUrl(apiUrl)
      .withCredentials(userName, password)
      .withExponentialBackoffRetry(BACKOFF_INITIAL_INTERVAL, BACKOFF_MULTIPLIER, BACKOFF_MAX_ATTEMPTS);
  }

  /**
   * Constructs an instance using properties class instance.
   */
  public RegistryClientUtil(Properties properties, String apiUrl) {
    clientBuilder
      .withUrl(apiUrl)
      .withCredentials(properties.getProperty(DownloadWorkflowModule.DefaultSettings.DOWNLOAD_USER_KEY),
                       properties.getProperty(DownloadWorkflowModule.DefaultSettings.DOWNLOAD_PASSWORD_KEY))
      .withExponentialBackoffRetry(BACKOFF_INITIAL_INTERVAL, BACKOFF_MULTIPLIER, BACKOFF_MAX_ATTEMPTS);
  }


  private static Properties loadConfig() {
    try {
      return PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);
    } catch (IOException ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  /**
   * Constructs an instance using the default properties file.
   */
  public RegistryClientUtil(String apiUrl) {
    this(loadConfig(), apiUrl);
  }

  /**
   * Constructs an instance using the default properties file.
   */
  public RegistryClientUtil() {
    this(loadConfig(), null);
  }

  /**
   * Sets up a registry DatasetService client avoiding the use of guice as our gbif jackson libraries clash with the
   * hadoop versions.
   * Sets up an http client with a one minute timeout and http support only.
   */
  public DatasetService setupDatasetService() {
    return clientBuilder.build(DatasetClient.class);
  }

  /**
   * Sets up a DatasetOccurrenceDownloadUsageService client avoiding the use of guice as our gbif jackson libraries
   * clash with the hadoop versions.
   * Sets up an http client with a one minute timeout and http support only.
   */
  public DatasetOccurrenceDownloadUsageService setupDatasetUsageService() {
    return clientBuilder.build(DatasetOccurrenceDownloadUsageClient.class);
  }

  /**
   * Sets up a OccurrenceDownloadService client avoiding the use of guice as our gbif jackson libraries
   * clash with the hadoop versions.
   * Sets up an http client with a one minute timeout and http support only.
   */
  public OccurrenceDownloadService setupOccurrenceDownloadService() {
    return clientBuilder.build(OccurrenceDownloadClient.class);
  }

}
