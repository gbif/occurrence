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

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.vocabulary.Extension;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.occurrence.mail.EmailSender;
import org.gbif.occurrence.mail.OccurrenceEmailManager;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class CallbackServiceTest {
  private static final String DOWNLOAD_ID = "0000000-120518122602221";
  private static final String JOB_ID = DOWNLOAD_ID + "-oozie-oozi-W";
  private static final String KILLED = "KILLED";
  private static final String FAILED = "FAILED";
  private static final String SUCCEEDED = "SUCCEEDED";
  private static final String RUNNING = "RUNNING";
  private static final Predicate DEFAULT_TEST_PREDICATE =
      new EqualsPredicate(OccurrenceSearchParameter.CATALOG_NUMBER, "bar", false);
  private static final String TEST_USER = "admin";
  private static final List<String> EMAILS = Collections.singletonList("tests@gbif.org");

  private OozieClient oozieClient;
  private DownloadRequestServiceImpl service;
  private OccurrenceDownloadService occurrenceDownloadService;
  private OccurrenceEmailManager emailManager;
  private EmailSender emailSender;
  private DownloadLimitsService downloadLimitsService;
  private MessagePublisher messagePublisher;

  /** Creates a mock download object. */
  private static Download mockDownload() {
    DownloadRequest downloadRequest =
        new PredicateDownloadRequest(
            DEFAULT_TEST_PREDICATE,
            TEST_USER,
            EMAILS,
            true,
            DownloadFormat.DWCA,
            DownloadType.OCCURRENCE,
            Collections.singleton(Extension.AUDUBON));
    Download download = new Download();
    download.setRequest(downloadRequest);
    download.setKey(DOWNLOAD_ID);
    download.setDownloadLink(JOB_ID + ".zip");
    download.setCreated(new Date());
    download.setModified(new Date());
    download.setStatus(Status.PREPARING);
    return download;
  }

  @BeforeEach
  void setup() throws OozieClientException {
    emailManager = mock(OccurrenceEmailManager.class);
    emailSender = mock(EmailSender.class);
    occurrenceDownloadService = mock(OccurrenceDownloadService.class);
    downloadLimitsService = mock(DownloadLimitsService.class);
    messagePublisher = mock(MessagePublisher.class);
    oozieClient = mock(OozieClient.class);

    when(downloadLimitsService.exceedsSimultaneousDownloadLimit(any(String.class)))
        .thenReturn(null);
    when(downloadLimitsService.exceedsDownloadComplexity(any(DownloadRequest.class)))
        .thenReturn(null);
    when(occurrenceDownloadService.get(anyString())).thenReturn(mockDownload());

    WorkflowJob job = mock(WorkflowJob.class);
    when(oozieClient.getJobInfo(JOB_ID)).thenReturn(job);
    when(job.getExternalId()).thenReturn(DOWNLOAD_ID);

    service =
        new DownloadRequestServiceImpl(
            oozieClient,
            "http://gbif-dev.org/occurrence",
            "http://localhost:8080/",
            "",
            occurrenceDownloadService,
            downloadLimitsService,
            emailManager,
            emailSender,
            messagePublisher);
  }

  @Test
  void testIgnoreRunningJobs() {
    service.processCallback(JOB_ID, RUNNING);
    verifyNoMoreInteractions(emailManager);
    verifyNoMoreInteractions(emailSender);
    verify(occurrenceDownloadService).update(any());
  }

  @Test
  void testIgnoreWrongStatuses() {
    assertThrows(IllegalArgumentException.class, () -> service.processCallback(JOB_ID, "INVALID"));
    verifyNoMoreInteractions(emailManager);
    verifyNoMoreInteractions(emailSender);
  }

  @Test
  void testNotificationSent() throws OozieClientException {
    WorkflowJob job = mock(WorkflowJob.class);
    when(oozieClient.getJobInfo(JOB_ID)).thenReturn(job);
    when(job.getExternalId()).thenReturn(DOWNLOAD_ID);
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

    verify(emailSender).send(any());
  }

  @Test
  void testNotifyAdminForKilledJobs() {
    Logger logger = (Logger) LoggerFactory.getLogger(DownloadRequestServiceImpl.class);
    // create and start a ListAppender
    ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
    listAppender.setName("ListAppender");
    listAppender.setContext(logger.getLoggerContext());
    listAppender.start();

    logger.addAppender(listAppender);

    service.processCallback(JOB_ID, KILLED);
    assertTrue(
        listAppender.list.stream()
            .anyMatch(
                event ->
                    event.getMarker() != null
                        && Constants.NOTIFY_ADMIN == event.getMarker()
                        && event.getFormattedMessage().contains(DOWNLOAD_ID)
                        && event.getFormattedMessage().contains(KILLED)),
        "Not admin Marker found for JobId " + JOB_ID + " and Status " + KILLED);

    verify(emailManager).generateFailedDownloadEmailModel(any(), any());
    verify(emailSender).send(any());
  }

  @Test
  void testNotifyAdminForFailedJobs() {
    Logger logger = (Logger) LoggerFactory.getLogger(DownloadRequestServiceImpl.class);
    // create and start a ListAppender
    ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
    listAppender.setName("ListAppender");
    listAppender.setContext(logger.getLoggerContext());
    listAppender.start();

    logger.addAppender(listAppender);

    service.processCallback(JOB_ID, FAILED);
    assertTrue(
        listAppender.list.stream()
            .anyMatch(
                event ->
                    event.getMarker() != null
                        && Constants.NOTIFY_ADMIN == event.getMarker()
                        && event.getFormattedMessage().contains(DOWNLOAD_ID)
                        && event.getFormattedMessage().contains(FAILED)),
        "Not admin Marker found for JobId " + JOB_ID + " and Status " + FAILED);

    verify(emailManager).generateFailedDownloadEmailModel(any(), any());
    verify(emailSender).send(any());
  }
}
