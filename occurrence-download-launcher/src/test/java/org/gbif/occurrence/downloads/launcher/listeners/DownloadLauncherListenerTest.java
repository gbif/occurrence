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

import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.common.messaging.api.messages.DownloadLauncherMessage;
import org.gbif.occurrence.downloads.launcher.services.DownloadUpdaterService;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher;

import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;

public class DownloadLauncherListenerTest {

  @Mock
  private DownloadLauncher jobManager;
  @Mock
  private DownloadUpdaterService downloadUpdaterService;

  @Mock
  private LockerService lockerService;

  @InjectMocks
  private DownloadLauncherListener listener;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testHandleMessageRunningStatus() throws Exception {
    String downloadKey = "test-key";

    PredicateDownloadRequest request = new PredicateDownloadRequest();
    request.setCreator("testUser");
    request.setFormat(DownloadFormat.DWCA);
    request.setNotificationAddresses(Collections.singleton("testEmail"));
    request.setPredicate(
        new EqualsPredicate(
            OccurrenceSearchParameter.DATASET_KEY, UUID.randomUUID().toString(), false));

    DownloadLauncherMessage downloadLauncherMessage =
        new DownloadLauncherMessage(downloadKey, request);

    Mockito.when(jobManager.create(downloadLauncherMessage))
        .thenReturn(DownloadLauncher.JobStatus.RUNNING);
    Mockito.when(jobManager.getStatusByName(downloadKey)).thenReturn(Optional.of(Status.RUNNING));

    listener.handleMessage(downloadLauncherMessage);

    Mockito.verify(jobManager).create(downloadLauncherMessage);
    Mockito.verify(lockerService).lock(downloadKey, Thread.currentThread());
  }

  @Test(expected = AmqpRejectAndDontRequeueException.class)
  public void testHandleMessageFailedStatus() {
    String downloadKey = "test-key";

    PredicateDownloadRequest request = new PredicateDownloadRequest();
    request.setCreator("testUser");
    request.setFormat(DownloadFormat.DWCA);
    request.setNotificationAddresses(Collections.singleton("testEmail"));
    request.setPredicate(
        new EqualsPredicate(
            OccurrenceSearchParameter.DATASET_KEY, UUID.randomUUID().toString(), false));

    DownloadLauncherMessage downloadLauncherMessage =
        new DownloadLauncherMessage(downloadKey, request);

    Mockito.when(jobManager.create(downloadLauncherMessage))
        .thenReturn(DownloadLauncher.JobStatus.FAILED);

    Mockito.when(downloadUpdaterService.isStatusFinished(downloadKey))
        .thenReturn(Boolean.TRUE);

    listener.handleMessage(downloadLauncherMessage);

    Mockito.verify(jobManager).create(downloadLauncherMessage);
    Mockito.verify(downloadUpdaterService).isStatusFinished(downloadKey);
  }
}
