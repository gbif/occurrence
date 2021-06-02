package org.gbif.occurrence.ws.resources;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.gbif.api.model.common.GbifUser;
import org.gbif.api.model.common.GbifUserPrincipal;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.service.CallbackService;

import java.security.Principal;
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

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
    ResponseEntity<String> response = resource.startDownload(dl, principal);
    assertThat(response.getBody(), equalTo(JOB_ID));
  }

  @Test
  public void testStartDownloadNotAuthenticated() {
    prepareMocks("foo");
    ResponseEntity<String> response = resource.startDownload(dl, principal);
    assertEquals(HttpStatus.UNAUTHORIZED, response.getStatusCode());
  }

  private void prepareMocks(String user) {
    String archiveServerUrl = "http://test/";
    CallbackService callbackService = mock(CallbackService.class);
    DownloadRequestService service = mock(DownloadRequestService.class);
    OccurrenceDownloadService downloadService = mock(OccurrenceDownloadService.class);

    GbifUser gbifUser = new GbifUser();
    gbifUser.setUserName(user);
    principal = new GbifUserPrincipal(gbifUser);

    Authentication auth = mock(Authentication.class);
    SecurityContextHolder.getContext().setAuthentication(auth);

    resource = new DownloadResource(archiveServerUrl, service, callbackService, downloadService);
    dl = new PredicateDownloadRequest(new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "1", false), USER, null, true,
      DownloadFormat.DWCA);

    PagingResponse<Download> empty = new PagingResponse<>();
    empty.setResults(Collections.EMPTY_LIST);
    when(downloadService.listByUser(any(), any(), any())).thenReturn(empty);
    when(service.create(dl)).thenReturn(JOB_ID);
  }
}
