package org.gbif.occurrence.ws.client.mock;

import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.api.service.occurrence.OccurrenceService;

import com.google.inject.PrivateModule;
import com.google.inject.Scopes;

public class OccurrencePersistenceMockModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(OccurrenceService.class).to(OccurrencePersistenceMockService.class).in(Scopes.SINGLETON);
    bind(OccurrenceSearchService.class).to(OccurrenceSearchMockService.class).in(Scopes.SINGLETON);
    expose(OccurrenceService.class);
    expose(OccurrenceSearchService.class);
  }
}
