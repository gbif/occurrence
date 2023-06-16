package org.gbif.occurrence.downloads.launcher.listeners;

import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.common.messaging.api.messages.DownloadLauncherMessage;
import org.gbif.occurrence.downloads.launcher.services.DownloadUpdaterService;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;

public class DownloadLauncherListenerTest {

  private DownloadLauncher jobManager;
  private DownloadUpdaterService downloadUpdaterService;
  private LockerService lockerService;
  private DownloadLauncherListener listener;

  @Before
  public void setUp() {
    jobManager = Mockito.mock(DownloadLauncher.class);
    downloadUpdaterService = Mockito.mock(DownloadUpdaterService.class);
    lockerService = Mockito.mock(LockerService.class);
    listener = new DownloadLauncherListener(jobManager, downloadUpdaterService, lockerService);
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

    listener.handleMessage(downloadLauncherMessage);

    Mockito.verify(jobManager).create(downloadLauncherMessage);
    Mockito.verify(downloadUpdaterService).updateStatus(downloadKey, Status.FAILED);
  }
}
