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

import org.gbif.common.messaging.api.messages.DownloadCancelMessage;
import org.gbif.occurrence.downloads.launcher.services.DownloadUpdaterService;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class DownloadCancellationListenerTest {

  private DownloadLauncher jobManager;
  private DownloadUpdaterService downloadUpdaterService;
  private LockerService lockerService;
  private DownloadCancellationListener listener;

  @BeforeEach
  public void setUp() {
    jobManager = Mockito.mock(DownloadLauncher.class);
    downloadUpdaterService = Mockito.mock(DownloadUpdaterService.class);
    lockerService = Mockito.mock(LockerService.class);
    listener = new DownloadCancellationListener(jobManager, downloadUpdaterService, lockerService);
  }

  @Test
  void testHandleMessage() {
    String downloadKey = "test-key";
    DownloadCancelMessage downloadCancelMessage = new DownloadCancelMessage(downloadKey);

    Mockito.when(jobManager.cancelRun(downloadKey)).thenReturn(DownloadLauncher.JobStatus.CANCELLED);

    listener.handleMessage(downloadCancelMessage);

    Mockito.verify(jobManager).cancelRun(downloadKey);
    Mockito.verify(lockerService).unlock(downloadKey);
    Mockito.verify(downloadUpdaterService).markAsCancelled(downloadKey);
  }
}
