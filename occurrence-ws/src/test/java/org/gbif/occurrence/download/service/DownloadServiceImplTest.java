package org.gbif.occurrence.download.service;

import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
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

  private static final Predicate DEFAULT_TEST_PREDICATE = new EqualsPredicate(PARAM, "bar");

  @Before
  public void setup() {
    props.clear();
    downloadService = mock(OccurrenceDownloadService.class);
    downloadLimitsService = mock(DownloadLimitsService.class);
    when(downloadLimitsService.isInDownloadLimits(any(String.class))).thenReturn(true);
    requestService =
      new DownloadRequestServiceImpl(oozieClient, props, "", "", downloadService, mock(DownloadEmailUtils.class), downloadLimitsService);
  }


  @Test
  public void testCreate() throws OozieClientException {
    when(oozieClient.run(any(Properties.class))).thenReturn(JOB_ID);
    DownloadRequest dl = new DownloadRequest(DEFAULT_TEST_PREDICATE, "markus", null, true, DownloadFormat.DWCA);
    String id = requestService.create(dl);

    assertThat(id, equalTo(DOWNLOAD_ID));
  }

  @Test
  @Ignore("See OCC-55: At the moment failures are not propagated")
  public void testFailedCreate() throws OozieClientException {
    doThrow(new OozieClientException("foo", "bar")).when(oozieClient).run(any(Properties.class));
    DownloadRequest dl = new DownloadRequest(DEFAULT_TEST_PREDICATE, "markus", null, true, DownloadFormat.DWCA);
    requestService.create(dl);

    // TODO: Assert on exception
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
    when(downloadService.listByUser(any(String.class), any(Pageable.class), Matchers.anySetOf(Download.Status.class))).thenReturn(
      new PagingResponse<Download>(0L, 0, 0L, emptyDownloads));
    when(downloadService.listByUser(eq("peter"), any(Pageable.class), Matchers.anySetOf(Download.Status.class))).thenReturn(
      new PagingResponse<Download>(0L, peterDownloads.size(), new Long(peterDownloads.size()), peterDownloads));
    when(downloadService.listByUser(eq("karl"), any(Pageable.class), Matchers.anySetOf(Download.Status.class))).thenReturn(
      new PagingResponse<Download>(0L, peterDownloads.size(), new Long(peterDownloads.size()), karlDownloads));
    when(downloadService.list(any(Pageable.class), Matchers.anySetOf(Download.Status.class))).thenReturn(
      new PagingResponse<Download>(0L, allDownloads.size(), new Long(allDownloads.size()), allDownloads));
    // mock get details
    when(downloadService.get(eq("1-oozie-oozi-W"))).thenReturn(job1);
    when(downloadService.get(eq("2-oozie-oozi-W"))).thenReturn(job2);

    // test
    PagingRequest req = new PagingRequest(0, 2);
    PagingResponse<Download> x = downloadService.list(req,null);
    assertEquals(2, x.getResults().size());

    x = downloadService.listByUser("harald", req, null);
    assertEquals(0, x.getResults().size());

    x = downloadService.listByUser("karl", req, null);
    assertEquals(1, x.getResults().size());

    x = downloadService.listByUser("peter", req, null);
    assertEquals(1, x.getResults().size());
  }

  @Test
  public void testNotification() throws OozieClientException {
    when(oozieClient.run(any(Properties.class))).thenReturn(JOB_ID);
    DownloadRequest dl =
      new DownloadRequest(DEFAULT_TEST_PREDICATE, "markus", Lists.newArrayList(TEST_EMAIL), true, DownloadFormat.DWCA);

    String downloadKey = requestService.create(dl);
    assertThat(downloadKey, equalTo(DOWNLOAD_ID));

    ArgumentCaptor<Properties> argument = ArgumentCaptor.forClass(Properties.class);
    verify(oozieClient).run(argument.capture());
    assertThat(argument.getValue().getProperty(Constants.NOTIFICATION_PROPERTY), equalTo(TEST_EMAIL));
  }

  private Download mockDownload(String downloadKey, String creator) {
    DownloadRequest downloadRequest = new DownloadRequest(DEFAULT_TEST_PREDICATE, creator, null, true, DownloadFormat.DWCA);
    Download download = new Download();
    download.setRequest(downloadRequest);
    download.setKey(downloadKey);
    download.setCreated(new Date());
    download.setModified(new Date());
    download.setStatus(Status.SUCCEEDED);
    return download;
  }

}
