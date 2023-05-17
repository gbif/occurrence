package org.gbif.occurrence.downloads.launcher.services;

import static org.mockito.Mockito.*;

import java.util.Collections;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DownloadsUpdaterScheduledTaskTest {

  private DownloadUpdaterService downloadUpdaterService;
  private DownloadLauncher downloadLauncher;
  private LockerService lockerService;
  private DownloadsUpdaterScheduledTask downloadsUpdaterScheduledTask;

  @BeforeEach
  public void setup() {
    downloadUpdaterService = mock(DownloadUpdaterService.class);
    downloadLauncher = mock(DownloadLauncher.class);
    lockerService = mock(LockerService.class);

    downloadsUpdaterScheduledTask =
        new DownloadsUpdaterScheduledTask(downloadLauncher, downloadUpdaterService, lockerService);
  }

  @Test
  void testRenewedDownloadsStatuses() {
    Download download = new Download();
    download.setKey("testKey");
    download.setStatus(Status.RUNNING);

    when(downloadUpdaterService.getExecutingDownloads())
        .thenReturn(Collections.singletonList(download));
    when(downloadLauncher.renewRunningDownloadsStatuses(anyList()))
        .thenReturn(Collections.singletonList(download));

    downloadsUpdaterScheduledTask.renewedDownloadsStatuses();

    verify(downloadUpdaterService, times(1)).getExecutingDownloads();
    verify(downloadLauncher, times(1)).renewRunningDownloadsStatuses(anyList());
    verify(downloadUpdaterService, times(1)).updateDownload(download);
    verify(lockerService, times(0)).unlock(download.getKey());
    verify(lockerService, times(1)).printLocks();
  }
}
