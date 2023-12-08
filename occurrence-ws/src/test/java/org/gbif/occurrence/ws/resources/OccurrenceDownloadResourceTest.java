/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.ws.resources;

import org.gbif.api.model.common.GbifUser;
import org.gbif.api.model.common.GbifUserPrincipal;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.vocabulary.Extension;
import org.gbif.occurrence.download.service.CallbackService;

import java.security.Principal;
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.server.ResponseStatusException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OccurrenceDownloadResourceTest {

  private static final String JOB_ID = "FOO";
  private static final String USER = "testuser";
  private static final String STATUS = "SUCCEEDED";

  private OccurrenceDownloadResource resource;
  private PredicateDownloadRequest dl;
  private Principal principal;

  @Test
  public void testCallback() {
    prepareMocks(USER, false);
    ResponseEntity<?> response = resource.oozieCallback(JOB_ID, STATUS);
    assertThat(response.getStatusCode(), equalTo(HttpStatus.OK));
  }

  @Test
  public void testStartDownload() {
    prepareMocks(USER, false);
    ResponseEntity<String> response = resource.startDownload(dl, null, principal, null);
    assertThat(response.getBody(), equalTo(JOB_ID));
  }

  @Test
  public void testStartDownloadNotAuthenticated() {
    prepareMocks("foo", false);
    ResponseEntity<String> response = resource.startDownload(dl, null, principal, null);
    assertEquals(HttpStatus.UNAUTHORIZED, response.getStatusCode());
  }

  @Test
  public void testStartDownloadFailedCreation() {
    prepareMocks(USER, true);
    ResponseEntity<String> response = resource.startDownload(dl, null, principal, null);
    assertEquals(HttpStatus.METHOD_FAILURE, response.getStatusCode());
  }

  private void prepareMocks(String user, boolean failedCreation) {
    String archiveServerUrl = "http://test/";
    CallbackService callbackService = mock(CallbackService.class);
    DownloadRequestService service = mock(DownloadRequestService.class);
    OccurrenceDownloadService downloadService = mock(OccurrenceDownloadService.class);

    GbifUser gbifUser = new GbifUser();
    gbifUser.setUserName(user);
    principal = new GbifUserPrincipal(gbifUser);

    Authentication auth = mock(Authentication.class);
    SecurityContextHolder.getContext().setAuthentication(auth);

    resource =
        new OccurrenceDownloadResource(
            archiveServerUrl, service, callbackService, downloadService, false);
    dl =
        new PredicateDownloadRequest(
            new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "1", false),
            USER,
            null,
            true,
            DownloadFormat.DWCA,
            DownloadType.OCCURRENCE,
            Collections.singleton(Extension.AUDUBON));

    PagingResponse<Download> empty = new PagingResponse<>();
    empty.setResults(Collections.emptyList());
    when(downloadService.listByUser(any(), any(), any(), any(), any())).thenReturn(empty);
    if (!failedCreation) {
      when(service.create(dl, null)).thenReturn(JOB_ID);
    } else {
      when(service.create(dl, null))
          .thenThrow(
              new ResponseStatusException(
                  HttpStatus.METHOD_FAILURE, "A download limitation is exceeded"));
    }
  }
}
