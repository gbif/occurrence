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
package org.gbif.occurrence.downloads.launcher.listeners;

import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.common.messaging.api.messages.DownloadLauncherMessage;
import org.gbif.occurrence.downloads.launcher.services.EventDownloadUpdaterService;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.gbif.occurrence.downloads.launcher.services.OccurrenceDownloadUpdaterService;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher.JobStatus;
import org.gbif.occurrence.downloads.launcher.services.launcher.EventDownloadLauncherService;
import org.gbif.occurrence.downloads.launcher.services.launcher.OccurrenceDownloadLauncherService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;

class DownloadLauncherListenerTest {

  @Mock private OccurrenceDownloadLauncherService occurrenceDownloadLauncherService;
  @Mock private EventDownloadLauncherService eventDownloadLauncherService;
  @Mock private OccurrenceDownloadUpdaterService occurrenceDownloadUpdaterService;
  @Mock private EventDownloadUpdaterService eventDownloadUpdaterService;

  @Mock private LockerService lockerService;

  @InjectMocks private DownloadLauncherListener listener;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  void testHandleMessageRunningStatus() throws Exception {
    String downloadKey = "test-key";

    PredicateDownloadRequest request = new PredicateDownloadRequest();
    request.setCreator("testUser");
    request.setFormat(DownloadFormat.DWCA);
    request.setNotificationAddresses(Collections.singleton("testEmail"));
    request.setType(DownloadType.OCCURRENCE);
    request.setPredicate(
        new EqualsPredicate(
            OccurrenceSearchParameter.DATASET_KEY, UUID.randomUUID().toString(), false));

    DownloadLauncherMessage downloadLauncherMessage =
        new DownloadLauncherMessage(downloadKey, request);

    Mockito.when(occurrenceDownloadLauncherService.createRun(downloadKey))
        .thenReturn(DownloadLauncher.JobStatus.RUNNING);
    Mockito.when(occurrenceDownloadLauncherService.getStatusByName(downloadKey))
        .thenReturn(Optional.of(Status.RUNNING));

    listener.handleMessage(downloadLauncherMessage);

    Mockito.verify(occurrenceDownloadLauncherService).createRun(downloadKey);
    Mockito.verify(lockerService).lock(downloadKey, Thread.currentThread());
  }

  @Test
  void testHandleMessageFailedStatus() {
    String downloadKey = "test-key";

    PredicateDownloadRequest request = new PredicateDownloadRequest();
    request.setCreator("testUser");
    request.setFormat(DownloadFormat.DWCA);
    request.setNotificationAddresses(Collections.singleton("testEmail"));
    request.setType(DownloadType.OCCURRENCE);
    request.setPredicate(
        new EqualsPredicate(
            OccurrenceSearchParameter.DATASET_KEY, UUID.randomUUID().toString(), false));

    DownloadLauncherMessage downloadLauncherMessage =
        new DownloadLauncherMessage(downloadKey, request);

    Mockito.when(occurrenceDownloadLauncherService.createRun(downloadKey))
        .thenReturn(DownloadLauncher.JobStatus.FAILED);

    Mockito.when(occurrenceDownloadUpdaterService.isStatusFinished(downloadKey))
        .thenReturn(Boolean.TRUE);

    listener.handleMessage(downloadLauncherMessage);

    Mockito.verifyNoInteractions(occurrenceDownloadLauncherService);
  }

  @Test
  void testHandleMessageAlreadyFinishedStatus() {
    String downloadKey = "test-key";

    PredicateDownloadRequest request = new PredicateDownloadRequest();
    request.setCreator("testUser");
    request.setFormat(DownloadFormat.DWCA);
    request.setNotificationAddresses(Collections.singleton("testEmail"));
    request.setType(DownloadType.OCCURRENCE);
    request.setPredicate(
        new EqualsPredicate(
            OccurrenceSearchParameter.DATASET_KEY, UUID.randomUUID().toString(), false));

    DownloadLauncherMessage downloadLauncherMessage =
        new DownloadLauncherMessage(downloadKey, request);

    Mockito.when(occurrenceDownloadLauncherService.createRun(downloadKey))
        .thenReturn(JobStatus.FINISHED);

    listener.handleMessage(downloadLauncherMessage);

    Mockito.verify(occurrenceDownloadUpdaterService).updateStatus(downloadKey, Status.SUCCEEDED);
  }

  @Test
  void testHandleMessageAlreadyFailedStatus() {
    Assertions.assertThrows(
        AmqpRejectAndDontRequeueException.class,
        () -> {
          String downloadKey = "test-key";

          PredicateDownloadRequest request = new PredicateDownloadRequest();
          request.setCreator("testUser");
          request.setFormat(DownloadFormat.DWCA);
          request.setNotificationAddresses(Collections.singleton("testEmail"));
          request.setType(DownloadType.OCCURRENCE);
          request.setPredicate(
              new EqualsPredicate(
                  OccurrenceSearchParameter.DATASET_KEY, UUID.randomUUID().toString(), false));

          DownloadLauncherMessage downloadLauncherMessage =
              new DownloadLauncherMessage(downloadKey, request);

          Mockito.when(occurrenceDownloadLauncherService.createRun(downloadKey))
              .thenReturn(JobStatus.FAILED);

          listener.handleMessage(downloadLauncherMessage);

          Mockito.verify(occurrenceDownloadUpdaterService).updateStatus(downloadKey, Status.FAILED);
        });
  }
}
