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
import org.gbif.occurrence.mail.EmailSender;
import org.gbif.occurrence.mail.OccurrenceEmailManager;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DownloadServiceImplTest {

  private static final String DOWNLOAD_ID = "123456789";
  private static final String JOB_ID = DOWNLOAD_ID + "-oozie-oozi-W";
  private static final OccurrenceSearchParameter PARAM = OccurrenceSearchParameter.CATALOG_NUMBER;
  private static final String TEST_EMAIL = "test@test.gbif.org";

  @Mock
  private OozieClient oozieClient;

  private final Map<String, String> props = Maps.newHashMap();

  private DownloadRequestService requestService;

  private OccurrenceDownloadService downloadService;

  private DownloadLimitsService downloadLimitsService;

  private static final Predicate DEFAULT_TEST_PREDICATE = new EqualsPredicate(PARAM, "bar", false);

  @BeforeEach
  public void setup() {
    props.clear();
    downloadService = mock(OccurrenceDownloadService.class);
    downloadLimitsService = mock(DownloadLimitsService.class);
    requestService =
      new DownloadRequestServiceImpl(oozieClient, props, "", "", "", downloadService, downloadLimitsService, mock(OccurrenceEmailManager.class), mock(EmailSender.class));
  }


  @Test
  public void testCreate() throws OozieClientException {
    when(oozieClient.run(any(Properties.class))).thenReturn(JOB_ID);
    DownloadRequest dl = new PredicateDownloadRequest(DEFAULT_TEST_PREDICATE, "markus", null, true, DownloadFormat.DWCA, DownloadType.OCCURRENCE);
    String id = requestService.create(dl);

    assertThat(id, equalTo(DOWNLOAD_ID));
  }

  @Test
  public void testFailedCreate() throws OozieClientException {
    doThrow(new OozieClientException("foo", "bar")).when(oozieClient).run(any(Properties.class));
    DownloadRequest dl = new PredicateDownloadRequest(DEFAULT_TEST_PREDICATE, "markus", null, true, DownloadFormat.DWCA, DownloadType.OCCURRENCE);

    try {
      requestService.create(dl);
      fail();
    } catch (ServiceUnavailableException e) {
    }
  }

  @Test
  public void testList() {
    List<Download> peterDownloads = Lists.newArrayList();
    List<Download> karlDownloads = Lists.newArrayList();
    List<Download> allDownloads = Lists.newArrayList();
    List<Download> emptyDownloads = Lists.newArrayList();

    Download job1 = mockDownload("1-oozie-oozi-W", "peter");
    peterDownloads.add(job1);

    Download job2 = mockDownload("2-oozie-oozi-W", "karl");
    karlDownloads.add(job2);
    allDownloads.add(job1);
    allDownloads.add(job2);
    // always get 3 job infos until we hit an offset of 100
    when(downloadService.listByUser(any(String.class), any(Pageable.class), ArgumentMatchers.anySet())).thenReturn(
      new PagingResponse<>(0L, 0, 0L, emptyDownloads));
    when(downloadService.listByUser(eq("peter"), any(Pageable.class), ArgumentMatchers.anySet())).thenReturn(
      new PagingResponse<>(0L, peterDownloads.size(), (long)peterDownloads.size(), peterDownloads));
    when(downloadService.listByUser(eq("karl"), any(Pageable.class), ArgumentMatchers.anySet())).thenReturn(
      new PagingResponse<>(0L, peterDownloads.size(), (long)peterDownloads.size(), karlDownloads));
    when(downloadService.list(any(Pageable.class), ArgumentMatchers.anySet())).thenReturn(
      new PagingResponse<>(0L, allDownloads.size(), (long)allDownloads.size(), allDownloads));

    // test
    PagingRequest req = new PagingRequest(0, 2);
    PagingResponse<Download> x = downloadService.list(req, Collections.emptySet());
    assertEquals(2, x.getResults().size());

    x = downloadService.listByUser("harald", req, Collections.emptySet());
    assertEquals(0, x.getResults().size());

    x = downloadService.listByUser("karl", req, Collections.emptySet());
    assertEquals(1, x.getResults().size());

    x = downloadService.listByUser("peter", req, Collections.emptySet());
    assertEquals(1, x.getResults().size());
  }

  @Test
  public void testNotification() throws OozieClientException {
    when(oozieClient.run(any(Properties.class))).thenReturn(JOB_ID);
    DownloadRequest dl =
      new PredicateDownloadRequest(DEFAULT_TEST_PREDICATE, "markus", Lists.newArrayList(TEST_EMAIL), true, DownloadFormat.DWCA, DownloadType.OCCURRENCE);

    String downloadKey = requestService.create(dl);
    assertThat(downloadKey, equalTo(DOWNLOAD_ID));

    ArgumentCaptor<Properties> argument = ArgumentCaptor.forClass(Properties.class);
    verify(oozieClient).run(argument.capture());
    assertThat(argument.getValue().getProperty(Constants.NOTIFICATION_PROPERTY), equalTo(TEST_EMAIL));
  }

  private Download mockDownload(String downloadKey, String creator) {
    DownloadRequest downloadRequest = new PredicateDownloadRequest(DEFAULT_TEST_PREDICATE, creator, null, true, DownloadFormat.DWCA, DownloadType.OCCURRENCE);
    Download download = new Download();
    download.setRequest(downloadRequest);
    download.setKey(downloadKey);
    download.setCreated(new Date());
    download.setModified(new Date());
    download.setStatus(Status.SUCCEEDED);
    return download;
  }

}
