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
package org.gbif.occurrence.downloads.launcher.services;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher;

import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

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
