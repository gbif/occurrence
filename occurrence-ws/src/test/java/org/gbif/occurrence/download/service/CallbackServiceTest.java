package org.gbif.occurrence.download.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import java.util.Date;
import java.util.List;
import javax.mail.MessagingException;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
                                                                              "bar", false);
  private static final String TEST_USER = "admin";
  private static final List<String> EMAILS = Lists.newArrayList("tests@gbif.org");

  private OozieClient oozieClient;
  private CallbackService service;
  private OccurrenceDownloadService occurrenceDownloadService;
  private DownloadEmailUtils downloadEmailUtils;
  private DownloadLimitsService downloadLimitsService;

  /**
   * Creates a mock download object.
   */
  private static Download mockDownload() {
    DownloadRequest downloadRequest = new PredicateDownloadRequest(DEFAULT_TEST_PREDICATE, TEST_USER, EMAILS, true,
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
    downloadLimitsService= mock(DownloadLimitsService.class);
    when(downloadLimitsService.exceedsSimultaneousDownloadLimit(any(String.class))).thenReturn(null);
    when(downloadLimitsService.exceedsDownloadComplexity(any(DownloadRequest.class))).thenReturn(null);
    when(occurrenceDownloadService.get(anyString())).thenReturn(mockDownload());
    oozieClient = mock(OozieClient.class);
    service =
      new DownloadRequestServiceImpl(oozieClient, Maps.newHashMap(), "http://localhost:8080/",
        "", occurrenceDownloadService, downloadEmailUtils,downloadLimitsService);
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

    Logger logger = (Logger)LoggerFactory.getLogger(DownloadRequestServiceImpl.class);
    // create and start a ListAppender
    ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
    listAppender.setName("ListAppender");
    listAppender.setContext(logger.getLoggerContext());
    listAppender.start();

    logger.addAppender(listAppender);

    service.processCallback(JOB_ID, KILLED);
    Assertions.assertTrue(
    listAppender.list.stream().anyMatch(event -> event.getMarker() != null && Constants.NOTIFY_ADMIN == event.getMarker() && event.getFormattedMessage().contains(JOB_ID) && event.getFormattedMessage().contains(KILLED)),
    "Not admin Marker found for JobId " + JOB_ID + " and Status " + KILLED);

    service.processCallback(JOB_ID, FAILED);
    Assertions.assertTrue(
      listAppender.list.stream().anyMatch(event -> event.getMarker() != null && Constants.NOTIFY_ADMIN == event.getMarker() && event.getFormattedMessage().contains(JOB_ID) && event.getFormattedMessage().contains(FAILED)),
      "Not admin Marker found for JobId " + JOB_ID + " and Status " + FAILED);
  }

}
