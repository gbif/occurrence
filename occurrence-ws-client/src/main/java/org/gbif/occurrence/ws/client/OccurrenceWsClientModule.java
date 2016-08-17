package org.gbif.occurrence.ws.client;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.ws.client.guice.GbifWsClientModule;
import org.gbif.ws.mixin.LicenseMixin;

import java.util.Map;
import java.util.Properties;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;

/**
 * A complete guice module that exposes the full ChecklistBank API or a configurable subset.
 * In order to use this module an authentication module needs to be installed first, such as {@link org.gbif.ws.client.guice.AnonymousAuthModule}
 * for anonymous access or {@link org.gbif.ws.client.guice.GbifApplicationAuthModule} for trusted applications.
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
  protected Map<Class<?>, Class<?>> getMixIns() {
    return new ImmutableMap.Builder<Class<?>, Class<?>>().put(Occurrence.class, LicenseMixin.class).build();
  }

  @Override
  protected void configureClient() {
    bind(OccurrenceService.class).to(OccurrenceWsClient.class).in(Scopes.SINGLETON);
    bind(OccurrenceSearchService.class).to(OccurrenceWsSearchClient.class).in(Scopes.SINGLETON);
    bind(DownloadRequestService.class).to(OccurrenceDownloadWsClient.class).in(Scopes.SINGLETON);
    expose(DownloadRequestService.class);
    expose(OccurrenceService.class);
    expose(OccurrenceSearchService.class);
  }


}
