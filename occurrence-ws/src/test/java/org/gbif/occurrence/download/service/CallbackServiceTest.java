package org.gbif.occurrence.download.service;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.service.registry.OccurrenceDownloadService;

import java.util.Date;
import java.util.List;

import javax.mail.MessagingException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.mockpolicies.Slf4jMockPolicy;
import org.powermock.core.classloader.annotations.MockPolicy;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;


@MockPolicy(Slf4jMockPolicy.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*", "org.xml.*", "com.sun.org.apache.xerces.*", "org.w3c.dom.*"})
@RunWith(PowerMockRunner.class)
public class CallbackServiceTest {

  private static final String DOWNLOAD_ID = "0000092-120518122602221";
  private static final String JOB_ID = DOWNLOAD_ID + "-oozie-oozi-W";
  private static final String KILLED = "KILLED";
  private static final String FAILED = "FAILED";
  private static final String SUCCEEDED = "SUCCEEDED";
  private static final String RUNNING = "RUNNING";
  private static final Predicate DEFAULT_TEST_PREDICATE = new EqualsPredicate(OccurrenceSearchParameter.CATALOG_NUMBER,
    "bar");
  private static final String TEST_USER = "admin";
  private static final List<String> EMAILS = Lists.newArrayList("tests@gbif.org");

  private OozieClient oozieClient;
  private CallbackService service;
  private OccurrenceDownloadService occurrenceDownloadService;
  private DownloadEmailUtils downloadEmailUtils;

  /**
   * Creates a mock download object.
   */
  private static Download mockDownload() {
    DownloadRequest downloadRequest = new DownloadRequest(DEFAULT_TEST_PREDICATE, TEST_USER, EMAILS, true,
                                                          DownloadFormat.DWCA);
    Download download = new Download();
    download.setRequest(downloadRequest);
    download.setKey(DOWNLOAD_ID);
    download.setDownloadLink(JOB_ID + ".zip");
    download.setCreated(new Date());
    download.setModified(new Date());
    download.setStatus(Status.PREPARING);
    return download;
  }

  @Before
  public void setup() {
    downloadEmailUtils = mock(DownloadEmailUtils.class);
    occurrenceDownloadService = mock(OccurrenceDownloadService.class);
    when(occurrenceDownloadService.get(anyString())).thenReturn(mockDownload());
    oozieClient = mock(OozieClient.class);
    service =
      new DownloadRequestServiceImpl(oozieClient, Maps.<String, String>newHashMap(), Maps.<String, String>newHashMap(),
                                     "http://localhost:8080/", "", occurrenceDownloadService, downloadEmailUtils);
  }


  @Test
  public void testIgnoreRunningJobs() {
    service.processCallback(JOB_ID, RUNNING);
    verifyNoMoreInteractions(oozieClient);
  }


  @Test(expected = IllegalArgumentException.class)
  public void testIgnoreWrongStatuses() {
    service.processCallback(JOB_ID, "INVALID");
    verifyNoMoreInteractions(downloadEmailUtils);
  }

  @Test
  public void testNotificationSent() throws OozieClientException, MessagingException {
    WorkflowJob job = mock(WorkflowJob.class);
    when(oozieClient.getJobInfo(JOB_ID)).thenReturn(job);
    when(job.getId()).thenReturn(JOB_ID);
    when(job.getCreatedTime()).thenReturn(new Date());
    when(job.getConf())
      .thenReturn(
        "<configuration>"
          + "<property><name>"
          + Constants.USER_PROPERTY
          + "</name>"
          + "<value>test</value></property>"

          + "<property><name>"
          + Constants.NOTIFICATION_PROPERTY
          + "</name>"
          + "<value>test@gbif.org</value></property>"

          + "<property><name>"
          + Constants.FILTER_PROPERTY
          + "</name>"
          + "<value>{\"type\":\"equals\",\"key\":\"DATASET_KEY\",\"value\":\"8575f23e-f762-11e1-a439-00145eb45e9a\"}</value></property>"
          + "</configuration>");

    service.processCallback(JOB_ID, SUCCEEDED);
  }

  @Test
  public void testNotifyAdminForFailedJobs() {
    Logger logger = LoggerFactory.getLogger(CallbackService.class);
    service.processCallback(JOB_ID, KILLED);
    verify(logger).error(eq(Constants.NOTIFY_ADMIN), anyString(), eq(JOB_ID), eq(KILLED));
    reset(logger);

    service.processCallback(JOB_ID, FAILED);
    verify(logger).error(eq(Constants.NOTIFY_ADMIN), anyString(), eq(JOB_ID), eq(FAILED));
    reset(logger);
  }

}
