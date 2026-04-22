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

import org.gbif.api.model.Constants;
import org.gbif.api.model.common.GbifUser;
import org.gbif.api.model.common.GbifUserPrincipal;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.SqlDownloadRequest;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.DisjunctionPredicate;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.predicate.InPredicate;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.UserRole;
import org.gbif.occurrence.download.service.CallbackService;

import java.security.Principal;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Disabled;
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
  private SqlDownloadRequest sqlDl;
  private SqlDownloadRequest badSqlDl;
  private Principal principal;

  @Test
  public void testCallback() {
    prepareMocks(USER, false);
    ResponseEntity<?> response = resource.airflowCallback(JOB_ID, STATUS);
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

  @Disabled
  @Test
  public void testStartSqlDownload() {
    prepareMocks(USER, false);
    ResponseEntity<String> response = resource.startDownload(sqlDl, null, principal, null);
    assertThat(response.getBody(), equalTo(JOB_ID));
  }

  @Test
  public void testStartInvalidSqlDownload() {
    prepareMocks(USER, false);
    ResponseEntity<String> response = resource.startDownload(badSqlDl, null, principal, null);
    assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
  }

  @Test
  public void testSearchToSql() {
    prepareMocks(USER, false);

    PredicateDownloadRequest complex =
      new PredicateDownloadRequest(
        new DisjunctionPredicate(
          List.of(
            new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "1", false, Constants.NUB_DATASET_KEY.toString()),
            new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "A", false, Constants.COL_DATASET_KEY.toString()),
            new InPredicate(OccurrenceSearchParameter.TAXON_KEY, List.of("2", "3"), false, Constants.NUB_DATASET_KEY.toString()),
            new InPredicate(OccurrenceSearchParameter.TAXON_KEY, List.of("F", "N"), false, Constants.COL_DATASET_KEY.toString()),

            new EqualsPredicate(OccurrenceSearchParameter.LITHOSTRATIGRAPHY, "AGAC", false),
            new InPredicate(OccurrenceSearchParameter.LITHOSTRATIGRAPHY, List.of("AGAC", "CAGA"), false),

            new EqualsPredicate(OccurrenceSearchParameter.BIOSTRATIGRAPHY, "AGAC", false),
            new InPredicate(OccurrenceSearchParameter.BIOSTRATIGRAPHY, List.of("AGAC", "CAGA"), false),

            new EqualsPredicate(OccurrenceSearchParameter.TAXONOMIC_ISSUE, "AGAC", false),
            new InPredicate(OccurrenceSearchParameter.TAXONOMIC_ISSUE, List.of("AGAC", "CAGA"), false),

            new EqualsPredicate(OccurrenceSearchParameter.ASSOCIATED_SEQUENCES, "AGAC", false),
            new InPredicate(OccurrenceSearchParameter.ASSOCIATED_SEQUENCES, List.of("AGAC", "CAGA"), false)
          )
        ),
        USER,
        null,
        true,
        DownloadFormat.DWCA,
        DownloadType.OCCURRENCE,
        "testDescription",
        null,
        Collections.singleton(Extension.AUDUBON),
        null,
        null);

    ResponseEntity<Object> response = resource.downloadSqlPost(complex);
    System.out.println(response);
    assertThat(response.getStatusCode(), equalTo(HttpStatus.OK));
  }

  // Needs to be resolved as part of https://github.com/gbif/gbif-api/issues/199
  @Disabled
  @Test
  public void failingTestSearchToSql() {
    prepareMocks(USER, false);

    PredicateDownloadRequest complex =
      new PredicateDownloadRequest(
        new DisjunctionPredicate(
          List.of(
            new EqualsPredicate(OccurrenceSearchParameter.GEOREFERENCED_BY, "Matt", false),
            new InPredicate(OccurrenceSearchParameter.GEOREFERENCED_BY, List.of("Matt", "Blissett"), false),
            new EqualsPredicate(OccurrenceSearchParameter.HIGHER_GEOGRAPHY, "Land", false),
            new InPredicate(OccurrenceSearchParameter.HIGHER_GEOGRAPHY, List.of("Land", "Sea"), false),
            new EqualsPredicate(OccurrenceSearchParameter.GEOLOGICAL_TIME, "123.45", false),
            new InPredicate(OccurrenceSearchParameter.GEOLOGICAL_TIME, List.of("123.45", "678.90"), false),
            new EqualsPredicate(OccurrenceSearchParameter.TAXONOMIC_ISSUE, "Incorrect", false),
            new InPredicate(OccurrenceSearchParameter.TAXONOMIC_ISSUE, List.of("Incorrect", "Incorrect-ish"), false)
          )
        ),
        USER,
        null,
        true,
        DownloadFormat.DWCA,
        DownloadType.OCCURRENCE,
        "testDescription",
        null,
        Collections.singleton(Extension.AUDUBON),
        null,
        null);

    ResponseEntity<Object> response = resource.downloadSqlPost(complex);
    System.out.println(response);
    assertThat(response.getStatusCode(), equalTo(HttpStatus.OK));
  }

  private void prepareMocks(String user, boolean failedCreation) {
    String archiveServerUrl = "http://test/";
    CallbackService callbackService = mock(CallbackService.class);
    DownloadRequestService service = mock(DownloadRequestService.class);
    OccurrenceDownloadService downloadService = mock(OccurrenceDownloadService.class);

    GbifUser gbifUser = new GbifUser();
    gbifUser.setUserName(user);
    gbifUser.addRole(UserRole.REGISTRY_ADMIN);
    principal = new GbifUserPrincipal(gbifUser);

    Authentication auth = mock(Authentication.class);
    SecurityContextHolder.getContext().setAuthentication(auth);

    resource =
        new OccurrenceDownloadResource(
            archiveServerUrl,
            service,
            callbackService,
            downloadService,
            false,
            "defaultChecklistKey");
    dl =
        new PredicateDownloadRequest(
            new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "1", false),
            USER,
            null,
            true,
            DownloadFormat.DWCA,
            DownloadType.OCCURRENCE,
            "testDescription",
            null,
            Collections.singleton(Extension.AUDUBON),
            null,
            null);
    sqlDl =
        new SqlDownloadRequest(
           "SELECT gbifid FROM occurrence",
           USER,
           null,
           true,
          DownloadFormat.SQL_TSV_ZIP,
          DownloadType.OCCURRENCE,
          "testDescription",
          null,null);
    badSqlDl =
        new SqlDownloadRequest(
           "SELECT * FROM occurrence",
           USER,
           null,
           true,
          DownloadFormat.SQL_TSV_ZIP,
          DownloadType.OCCURRENCE,
          "testDescription",
          null,null);

    PagingResponse<Download> empty = new PagingResponse<>();
    empty.setResults(Collections.emptyList());
    when(downloadService.listByUser(any(), any(), any(), any(), any())).thenReturn(empty);
    if (!failedCreation) {
      when(service.create(dl, null)).thenReturn(JOB_ID);
      when(service.create(sqlDl, null)).thenReturn(JOB_ID);
      when(service.create(badSqlDl, null)).thenReturn(JOB_ID);
    } else {
      when(service.create(dl, null))
          .thenThrow(
              new ResponseStatusException(
                  HttpStatus.METHOD_FAILURE, "A download limitation is exceeded"));
    }
  }
}
