package org.gbif.metrics.ws.client.guice;

import org.gbif.api.service.metrics.CubeService;
import org.gbif.api.service.occurrence.OccurrenceCountryIndexService;
import org.gbif.api.service.occurrence.OccurrenceDatasetIndexService;
import org.gbif.api.service.occurrence.OccurrenceDistributionIndexService;
import org.gbif.metrics.ws.client.CubeWsClient;
import org.gbif.metrics.ws.client.OccurrenceCountryIndexWsClient;
import org.gbif.metrics.ws.client.OccurrenceDatasetIndexWsClient;
import org.gbif.metrics.ws.client.OccurrenceDistributionIndexWsClient;
import org.gbif.ws.client.guice.GbifWsClientModule;

import java.util.Properties;

import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;

/**
 * Guice module that exposes the Metrics Service.
 */
public class MetricsWsClientModule extends GbifWsClientModule {

  public MetricsWsClientModule(Properties properties) {
    super(properties, CubeWsClient.class.getPackage());
  }

  /**
   * @return the web resource to the webapp
   */
  @Provides
  @Singleton
  public WebResource provideWebResource(Client client, @Named("metrics.ws.url") String url) {
    return client.resource(url);
  }

  @Override
  protected void configureClient() {
    bind(CubeService.class).to(CubeWsClient.class).in(Scopes.SINGLETON);
    bind(OccurrenceDatasetIndexService.class).to(OccurrenceDatasetIndexWsClient.class).in(Scopes.SINGLETON);
    bind(OccurrenceCountryIndexService.class).to(OccurrenceCountryIndexWsClient.class).in(Scopes.SINGLETON);
    bind(OccurrenceDistributionIndexService.class).to(OccurrenceDistributionIndexWsClient.class).in(Scopes.SINGLETON);

    expose(CubeService.class);
    expose(OccurrenceDatasetIndexService.class);
    expose(OccurrenceCountryIndexService.class);
    expose(OccurrenceDistributionIndexService.class);
  }
}
