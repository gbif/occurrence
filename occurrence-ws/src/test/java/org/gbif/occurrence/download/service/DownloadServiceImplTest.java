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
package org.gbif.occurrence.download.service;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.vocabulary.Extension;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.occurrence.mail.EmailSender;
import org.gbif.occurrence.mail.OccurrenceEmailManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.oozie.client.OozieClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.server.ResponseStatusException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DownloadServiceImplTest {
  private static final Pattern REGEX = Pattern.compile("0000000-\\d{15}");
  private static final OccurrenceSearchParameter PARAM = OccurrenceSearchParameter.CATALOG_NUMBER;

  @Mock private OozieClient oozieClient;
  @Mock private OccurrenceDownloadService downloadService;
  @Mock private DownloadLimitsService downloadLimitsService;
  @Mock private OccurrenceEmailManager emailManager;
  @Mock private EmailSender emailSender;

  private DownloadRequestService requestService;

  private static final Predicate DEFAULT_TEST_PREDICATE = new EqualsPredicate(PARAM, "bar", false);

  @BeforeEach
  public void setup() {
    MessagePublisher messagePublisher = mock(MessagePublisher.class);
    requestService =
        new DownloadRequestServiceImpl(
            oozieClient,
            "",
            "",
            "",
            downloadService,
            downloadLimitsService,
            emailManager,
            emailSender,
            messagePublisher);
  }

  @Test
  void testCreate() {

    DownloadRequest dl =
        new PredicateDownloadRequest(
            DEFAULT_TEST_PREDICATE,
            "markus",
            null,
            true,
            DownloadFormat.DWCA,
            DownloadType.OCCURRENCE,
            Collections.singleton(Extension.AUDUBON));
    String id = requestService.create(dl, null);

    assertTrue(REGEX.matcher(id).matches());
  }

  @Test
  void testLimitationIsExceeded() {
    DownloadRequest dl =
        new PredicateDownloadRequest(
            DEFAULT_TEST_PREDICATE,
            "markus",
            null,
            true,
            DownloadFormat.DWCA,
            DownloadType.OCCURRENCE,
            Collections.singleton(Extension.AUDUBON));

    when(downloadLimitsService.exceedsDownloadComplexity(any())).thenReturn("test");

    try {
      requestService.create(dl, null);
      fail();
    } catch (ResponseStatusException e) {
      // NOP
    }
  }

  @Test
  void testList() {
    List<Download> peterDownloads = new ArrayList<>();
    List<Download> karlDownloads = new ArrayList<>();
    List<Download> allDownloads = new ArrayList<>();
    List<Download> emptyDownloads = new ArrayList<>();

    Download job1 = mockDownload("1-oozie-oozi-W", "peter");
    peterDownloads.add(job1);

    Download job2 = mockDownload("2-oozie-oozi-W", "karl");
    karlDownloads.add(job2);
    allDownloads.add(job1);
    allDownloads.add(job2);
    // always get 3 job infos until we hit an offset of 100
    when(downloadService.listByUser(
            any(String.class),
            any(Pageable.class),
            ArgumentMatchers.anySet(),
            any(Date.class),
            any(Boolean.class)))
        .thenReturn(new PagingResponse<>(0L, 0, 0L, emptyDownloads));
    when(downloadService.listByUser(
            eq("peter"),
            any(Pageable.class),
            ArgumentMatchers.anySet(),
            any(Date.class),
            any(Boolean.class)))
        .thenReturn(
            new PagingResponse<>(
                0L, peterDownloads.size(), (long) peterDownloads.size(), peterDownloads));
    when(downloadService.listByUser(
            eq("karl"),
            any(Pageable.class),
            ArgumentMatchers.anySet(),
            any(Date.class),
            any(Boolean.class)))
        .thenReturn(
            new PagingResponse<>(
                0L, peterDownloads.size(), (long) peterDownloads.size(), karlDownloads));
    when(downloadService.list(any(Pageable.class), ArgumentMatchers.anySet(), any()))
        .thenReturn(
            new PagingResponse<>(
                0L, allDownloads.size(), (long) allDownloads.size(), allDownloads));

    // test
    PagingRequest req = new PagingRequest(0, 2);
    PagingResponse<Download> x = downloadService.list(req, Collections.emptySet(), null);
    assertEquals(2, x.getResults().size());

    x = downloadService.listByUser("harald", req, Collections.emptySet(), new Date(), true);
    assertEquals(0, x.getResults().size());

    x = downloadService.listByUser("karl", req, Collections.emptySet(), new Date(), true);
    assertEquals(1, x.getResults().size());

    x = downloadService.listByUser("peter", req, Collections.emptySet(), new Date(), true);
    assertEquals(1, x.getResults().size());
  }

  private Download mockDownload(String downloadKey, String creator) {
    DownloadRequest downloadRequest =
        new PredicateDownloadRequest(
            DEFAULT_TEST_PREDICATE,
            creator,
            null,
            true,
            DownloadFormat.DWCA,
            DownloadType.OCCURRENCE,
            Collections.singleton(Extension.AUDUBON));
    Download download = new Download();
    download.setRequest(downloadRequest);
    download.setKey(downloadKey);
    download.setCreated(new Date());
    download.setModified(new Date());
    download.setStatus(Status.RUNNING);
    return download;
  }
}
