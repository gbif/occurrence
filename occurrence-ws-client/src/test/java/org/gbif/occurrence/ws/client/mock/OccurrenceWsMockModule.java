package org.gbif.occurrence.ws.client.mock;

import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.service.CallbackService;

import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import org.gbif.occurrence.persistence.experimental.OccurrenceRelationshipService;
import org.gbif.occurrence.search.OccurrenceGetByKey;
import org.mockito.Mockito;

public class OccurrenceWsMockModule extends PrivateModule {

  @Override
  protected void configure() {
    bind(OccurrenceService.class).to(OccurrencePersistenceMockService.class).in(Scopes.SINGLETON);
    bind(OccurrenceGetByKey.class).to(OccurrencePersistenceMockService.class).in(Scopes.SINGLETON);
    bind(OccurrenceSearchService.class).to(OccurrenceSearchMockService.class).in(Scopes.SINGLETON);
    bind(OccurrenceRelationshipService.class).to(OccurrenceSearchMockService.class).in(Scopes.SINGLETON);
    //Following mocked services are required to bind all the services expected by the client module
    bind(OccurrenceDownloadService.class).toInstance(Mockito.mock(OccurrenceDownloadService.class));
    bind(DownloadRequestService.class).toInstance(Mockito.mock(DownloadRequestService.class));
    bind(CallbackService.class).toInstance(Mockito.mock(CallbackService.class));
    expose(OccurrenceService.class);
    expose(OccurrenceGetByKey.class);
    expose(OccurrenceSearchService.class);
    expose(OccurrenceRelationshipService.class);
    expose(OccurrenceDownloadService.class);
    expose(DownloadRequestService.class);
    expose(CallbackService.class);
  }
}
