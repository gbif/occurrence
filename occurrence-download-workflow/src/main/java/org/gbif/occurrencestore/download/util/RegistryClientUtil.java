package org.gbif.occurrencestore.download.util;

import org.gbif.api.service.registry.DatasetOccurrenceDownloadUsageService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrencestore.download.oozie.JacksonJsonContextResolver;
import org.gbif.registry.ws.client.DatasetOccurrenceDownloadUsageWsClient;
import org.gbif.registry.ws.client.DatasetWsClient;
import org.gbif.registry.ws.client.OccurrenceDownloadWsClient;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.gbif.ws.client.guice.SingleUserAuthModule;

import java.io.IOException;
import java.util.Properties;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.ClientFilter;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.apache.ApacheHttpClient;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;

/**
 * Utility class to create registry web service clients.
 */
public class RegistryClientUtil {

  private static final int REGISTRY_CLIENT_TO = 600000; // registry client default timeout

  private static final String DOWNLOAD_USER_KEY = "occurrence-download.ws.username";
  private static final String DOWNLOAD_PASSWORD_KEY = "occurrence-download.ws.password";

  private final Injector injector;


  /**
   * Constructs an instance using properties class instance.
   */
  public RegistryClientUtil(Properties properties) {
    injector = Guice.createInjector(createAuthModuleInstance(properties));
  }


  /**
   * Constructs an instance using a properties file name.
   */
  public RegistryClientUtil(String propertiesFile) {
    try {
      injector = Guice.createInjector(createAuthModuleInstance(PropertiesUtil.loadProperties(propertiesFile)));
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Creates an HTTP client.
   */
  private static ApacheHttpClient createHttpClient() {
    ClientConfig cc = new DefaultClientConfig();
    cc.getClasses().add(JacksonJsonContextResolver.class);
    cc.getClasses().add(JacksonJsonProvider.class);
    cc.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, true);
    cc.getProperties().put(ClientConfig.PROPERTY_CONNECT_TIMEOUT, REGISTRY_CLIENT_TO);
    return ApacheHttpClient.create(cc);
  }

  /**
   * Sets up a registry DatasetService client avoiding the use of guice as our gbif jackson libraries clash with the
   * hadoop versions.
   * Sets up an http client with a one minute timeout and http support only.
   * 
   * @throws IOException
   */
  public DatasetService setupDatasetService(final String uri) {
    return new DatasetWsClient(createHttpClient().resource(uri), injector.getInstance(ClientFilter.class));
  }

  /**
   * Sets up a DatasetOccurrenceDownloadUsageService client avoiding the use of guice as our gbif jackson libraries
   * clash with the hadoop versions.
   * Sets up an http client with a one minute timeout and http support only.
   * 
   * @throws IOException
   */
  public DatasetOccurrenceDownloadUsageService setupDatasetUsageService(final String uri) {
    return new DatasetOccurrenceDownloadUsageWsClient(createHttpClient().resource(uri),
      injector.getInstance(ClientFilter.class));
  }


  /**
   * Sets up a OccurrenceDownloadService client avoiding the use of guice as our gbif jackson libraries
   * clash with the hadoop versions.
   * Sets up an http client with a one minute timeout and http support only.
   * 
   * @throws IOException
   */
  public OccurrenceDownloadService setupOccurrenceDownloadService(final String uri) {
    return new OccurrenceDownloadWsClient(createHttpClient().resource(uri),
      injector.getInstance(ClientFilter.class));
  }


  /**
   * Creates a instance of the gbif authentication module.
   */
  private AbstractModule createAuthModuleInstance(Properties properties) {
    return new SingleUserAuthModule(properties.getProperty(DOWNLOAD_USER_KEY),
      properties.getProperty(DOWNLOAD_PASSWORD_KEY));
  }
}
