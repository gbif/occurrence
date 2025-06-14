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

import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.common.messaging.api.messages.DownloadCancelMessage;
import org.gbif.occurrence.downloads.launcher.services.EventDownloadUpdaterService;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.gbif.occurrence.downloads.launcher.services.OccurrenceDownloadUpdaterService;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher;
import org.gbif.occurrence.downloads.launcher.services.launcher.EventDownloadLauncherService;
import org.gbif.occurrence.downloads.launcher.services.launcher.OccurrenceDownloadLauncherService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DownloadCancellationListenerTest {

  private OccurrenceDownloadLauncherService occurrenceDownloadLauncherService;
  private EventDownloadLauncherService eventDownloadLauncherService;
  private OccurrenceDownloadUpdaterService occurrenceDownloadUpdaterService;
  private EventDownloadUpdaterService eventDownloadUpdaterService;
  private LockerService lockerService;
  private DownloadCancellationListener listener;

  @BeforeEach
  public void setUp() {
    occurrenceDownloadLauncherService = Mockito.mock(OccurrenceDownloadLauncherService.class);
    eventDownloadLauncherService = Mockito.mock(EventDownloadLauncherService.class);
    occurrenceDownloadUpdaterService = Mockito.mock(OccurrenceDownloadUpdaterService.class);
    eventDownloadUpdaterService = Mockito.mock(EventDownloadUpdaterService.class);
    lockerService = Mockito.mock(LockerService.class);
    listener =
        new DownloadCancellationListener(
            occurrenceDownloadLauncherService,
            eventDownloadLauncherService,
            occurrenceDownloadUpdaterService,
            eventDownloadUpdaterService,
            lockerService);
  }

  @Test
  void testHandleMessageOccurrences() {
    String downloadKey = "test-key";
    DownloadCancelMessage downloadCancelMessage =
        new DownloadCancelMessage(downloadKey, DownloadType.OCCURRENCE);

    Mockito.when(occurrenceDownloadLauncherService.cancelRun(downloadKey))
        .thenReturn(DownloadLauncher.JobStatus.CANCELLED);

    listener.handleMessage(downloadCancelMessage);

    Mockito.verify(occurrenceDownloadLauncherService).cancelRun(downloadKey);
    Mockito.verify(lockerService).unlock(downloadKey);
    Mockito.verify(occurrenceDownloadUpdaterService).markAsCancelled(downloadKey);
  }

  @Test
  void testHandleMessageEvents() {
    String downloadKey = "test-key";
    DownloadCancelMessage downloadCancelMessage =
        new DownloadCancelMessage(downloadKey, DownloadType.EVENT);

    Mockito.when(eventDownloadLauncherService.cancelRun(downloadKey))
        .thenReturn(DownloadLauncher.JobStatus.CANCELLED);

    listener.handleMessage(downloadCancelMessage);

    Mockito.verify(eventDownloadLauncherService).cancelRun(downloadKey);
    Mockito.verify(lockerService).unlock(downloadKey);
    Mockito.verify(eventDownloadUpdaterService).markAsCancelled(downloadKey);
  }
}
