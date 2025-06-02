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

import static org.mockito.Mockito.*;

import java.util.Collections;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.occurrence.downloads.launcher.services.launcher.EventDownloadLauncherService;
import org.gbif.occurrence.downloads.launcher.services.launcher.OccurrenceDownloadLauncherService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DownloadsUpdaterScheduledTaskTest {

  private OccurrenceDownloadLauncherService occurrenceDownloadLauncherService;
  private EventDownloadLauncherService eventDownloadLauncherService;
  private OccurrenceDownloadUpdaterService occurrenceDownloadUpdaterService;
  private EventDownloadUpdaterService eventDownloadUpdaterService;
  private LockerService lockerService;
  private DownloadsUpdaterScheduledTask downloadsUpdaterScheduledTask;

  @BeforeEach
  public void setup() {
    occurrenceDownloadLauncherService = mock(OccurrenceDownloadLauncherService.class);
    eventDownloadLauncherService = mock(EventDownloadLauncherService.class);
    occurrenceDownloadUpdaterService = mock(OccurrenceDownloadUpdaterService.class);
    eventDownloadUpdaterService = mock(EventDownloadUpdaterService.class);
    lockerService = mock(LockerService.class);

    downloadsUpdaterScheduledTask =
        new DownloadsUpdaterScheduledTask(
            occurrenceDownloadLauncherService,
            eventDownloadLauncherService,
            occurrenceDownloadUpdaterService,
            eventDownloadUpdaterService,
            lockerService);
  }

  @Test
  void testRenewedDownloadsStatuses() {
    Download download = new Download();
    download.setKey("testKey");
    download.setStatus(Status.RUNNING);

    when(occurrenceDownloadUpdaterService.getExecutingDownloads())
        .thenReturn(Collections.singletonList(download));
    when(occurrenceDownloadLauncherService.renewRunningDownloadsStatuses(anyList()))
        .thenReturn(Collections.singletonList(download));

    downloadsUpdaterScheduledTask.renewedDownloadsStatuses();

    verify(occurrenceDownloadUpdaterService, times(1)).getExecutingDownloads();
    verify(occurrenceDownloadLauncherService, times(1)).renewRunningDownloadsStatuses(anyList());
    verify(occurrenceDownloadUpdaterService, times(1)).updateDownload(download);
    verify(lockerService, times(0)).unlock(download.getKey());
    verify(lockerService, times(1)).printLocks();
  }
}
