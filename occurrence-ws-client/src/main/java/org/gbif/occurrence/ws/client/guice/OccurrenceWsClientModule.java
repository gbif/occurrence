package org.gbif.occurrence.ws.client.guice;

import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.api.service.occurrence.VerbatimOccurrenceService;
import org.gbif.occurrence.ws.client.OccurrenceWsClient;
import org.gbif.occurrence.ws.client.OccurrenceWsSearchClient;
import org.gbif.ws.client.guice.GbifWsClientModule;

import java.util.Properties;

import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;

/**
 * A complete guice module that exposes the full ChecklistBank API or a configurable subset.
 */
public class OccurrenceWsClientModule extends GbifWsClientModule {

  /**
   * Default module with the complete API exposed.
   */
  public OccurrenceWsClientModule(Properties properties) {
    super(properties, OccurrenceWsClient.class.getPackage());
  }

  /**
   * @param client for creating the occurrence web resource
   * @param url should point to the occurrence web service base url
   * @return the web resource to the occurrence webapp
   */
  @Provides
  @Singleton
  public WebResource provideWebResource(Client client, @Named("occurrence.ws.url") String url) {
    return client.resource(url);
  }

  @Override
  protected void configureClient() {
    bind(OccurrenceService.class).to(OccurrenceWsClient.class).in(Scopes.SINGLETON);
    bind(VerbatimOccurrenceService.class).to(OccurrenceWsClient.class).in(Scopes.SINGLETON);
    bind(OccurrenceSearchService.class).to(OccurrenceWsSearchClient.class).in(Scopes.SINGLETON);
    expose(OccurrenceService.class);
    expose(VerbatimOccurrenceService.class);
    expose(OccurrenceSearchService.class);
  }
}
