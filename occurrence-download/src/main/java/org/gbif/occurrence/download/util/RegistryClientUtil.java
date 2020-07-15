package org.gbif.occurrence.download.util;

import org.gbif.api.service.registry.DatasetOccurrenceDownloadUsageService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.registry.ws.client.DatasetClient;
import org.gbif.registry.ws.client.DatasetOccurrenceDownloadUsageClient;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.gbif.ws.client.ClientFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

/**
 * Utility class to create registry web service clients.
 */
public class RegistryClientUtil {

  private final ClientFactory clientFactory;

  public RegistryClientUtil(String userName, String password, String apiUrl) {
    clientFactory = new ClientFactory(userName, password, apiUrl);
  }

  /**
   * Constructs an instance using properties class instance.
   */
  public RegistryClientUtil(Properties properties, String apiUrl) {
    clientFactory = new ClientFactory(properties.getProperty(DownloadWorkflowModule.DefaultSettings.DOWNLOAD_USER_KEY),
                                      properties.getProperty(DownloadWorkflowModule.DefaultSettings.DOWNLOAD_PASSWORD_KEY),
                                      Optional.ofNullable(apiUrl).orElse(properties.getProperty(DownloadWorkflowModule.DefaultSettings.REGISTRY_URL_KEY)));
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
    return clientFactory.newInstance(DatasetClient.class);
  }

  /**
   * Sets up a DatasetOccurrenceDownloadUsageService client avoiding the use of guice as our gbif jackson libraries
   * clash with the hadoop versions.
   * Sets up an http client with a one minute timeout and http support only.
   */
  public DatasetOccurrenceDownloadUsageService setupDatasetUsageService() {
    return clientFactory.newInstance(DatasetOccurrenceDownloadUsageClient.class);
  }

  /**
   * Sets up a OccurrenceDownloadService client avoiding the use of guice as our gbif jackson libraries
   * clash with the hadoop versions.
   * Sets up an http client with a one minute timeout and http support only.
   */
  public OccurrenceDownloadService setupOccurrenceDownloadService() {
    return clientFactory.newInstance(OccurrenceDownloadClient.class);
  }

}
