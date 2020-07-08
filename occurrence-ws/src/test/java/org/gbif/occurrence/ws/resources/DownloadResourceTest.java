package org.gbif.occurrence.ws.resources;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.gbif.api.model.common.GbifUser;
import org.gbif.api.model.common.GbifUserPrincipal;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.service.CallbackService;

import java.security.Principal;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.server.ResponseStatusException;

public class DownloadResourceTest {

  private static final String JOB_ID = "FOO";
  private static final String USER = "testuser";
  private static final String STATUS = "SUCCEEDED";

  private DownloadResource resource;
  private PredicateDownloadRequest dl;
  private Principal principal;

  @Test
  public void testCallback() {
    prepareMocks(USER);
    ResponseEntity response = resource.oozieCallback(JOB_ID, STATUS);
    assertThat(response.getStatusCode(), equalTo(HttpStatus.OK));
  }

  @Test
  public void testStartDownload() {
    prepareMocks(USER);
    String jobId = resource.startDownload(dl, principal);
    assertThat(jobId, equalTo(JOB_ID));
  }

  @Test
  public void testStartDownloadNotAuthenticated() {
    Assertions.assertThrows(ResponseStatusException.class, () -> {
      prepareMocks("foo");
      resource.startDownload(dl, principal);
    });
  }

  private void prepareMocks(String user) {
    CallbackService callbackService = mock(CallbackService.class);
    DownloadRequestService service = mock(DownloadRequestService.class);
    OccurrenceDownloadService downloadService = mock(OccurrenceDownloadService.class);
    GbifUser gbifUser = new GbifUser();
    gbifUser.setUserName(user);
    principal = new GbifUserPrincipal(gbifUser);

    resource = new DownloadResource(service, callbackService, downloadService, null);
    dl = new PredicateDownloadRequest(new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "1"), USER, null, true,
      DownloadFormat.DWCA);
    when(service.create(dl)).thenReturn(JOB_ID);
  }

}
