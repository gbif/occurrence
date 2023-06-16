package org.gbif.occurrence.downloads.launcher.listeners;

import org.gbif.common.messaging.api.messages.DownloadCancelMessage;
import org.gbif.occurrence.downloads.launcher.services.DownloadUpdaterService;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class DownloadCancellationListenerTest {

  private DownloadLauncher jobManager;
  private DownloadUpdaterService downloadUpdaterService;
  private LockerService lockerService;
  private DownloadCancellationListener listener;

  @Before
  public void setUp() {
    jobManager = Mockito.mock(DownloadLauncher.class);
    downloadUpdaterService = Mockito.mock(DownloadUpdaterService.class);
    lockerService = Mockito.mock(LockerService.class);
    listener = new DownloadCancellationListener(jobManager, downloadUpdaterService, lockerService);
  }

  @Test
  public void testHandleMessage() {
    String downloadKey = "test-key";
    DownloadCancelMessage downloadCancelMessage = new DownloadCancelMessage(downloadKey);

    Mockito.when(jobManager.cancel(downloadKey)).thenReturn(DownloadLauncher.JobStatus.CANCELLED);

    listener.handleMessage(downloadCancelMessage);

    Mockito.verify(jobManager).cancel(downloadKey);
    Mockito.verify(lockerService).unlock(downloadKey);
    Mockito.verify(downloadUpdaterService).markAsCancelled(downloadKey);
  }
}
