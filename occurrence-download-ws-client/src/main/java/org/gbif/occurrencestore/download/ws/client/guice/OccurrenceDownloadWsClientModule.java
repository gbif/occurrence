package org.gbif.occurrencestore.download.ws.client.guice;

import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.occurrencestore.download.ws.client.OccurrenceDownloadWsClient;
import org.gbif.ws.client.guice.GbifWsClientModule;

import java.util.Properties;

import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;

/**
 * This module requires a ClientFilter to be bound in a separately installed authentication module.
 */
public class OccurrenceDownloadWsClientModule extends GbifWsClientModule {

  public OccurrenceDownloadWsClientModule(Properties properties) {
    super(properties);
  }

  @Provides
  @Singleton
  public WebResource providesDownloadWsWebResource(Client client, @Named("occurrencedownload.ws.url") String url) {
    return client.resource(url);
  }

  @Override
  protected void configureClient() {
    bind(DownloadRequestService.class).to(OccurrenceDownloadWsClient.class).in(Scopes.SINGLETON);
    expose(DownloadRequestService.class);
  }
}
