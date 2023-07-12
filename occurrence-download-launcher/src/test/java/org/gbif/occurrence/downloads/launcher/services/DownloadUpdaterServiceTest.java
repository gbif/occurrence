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
import org.gbif.registry.ws.client.OccurrenceDownloadClient;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class DownloadUpdaterServiceTest {

  private DownloadUpdaterService downloadUpdaterService;
  private OccurrenceDownloadClient mockClient;

  @BeforeEach
  public void setup() {
    mockClient = mock(OccurrenceDownloadClient.class);
    downloadUpdaterService = new DownloadUpdaterService(mockClient);
  }

  @Test
  void testUpdateStatus() {
    String downloadKey = "testKey";
    Download download = new Download();
    download.setStatus(Status.PREPARING);

    when(mockClient.get(downloadKey)).thenReturn(download);

    downloadUpdaterService.updateStatus(downloadKey, Status.RUNNING);

    verify(mockClient, times(1)).get(downloadKey);
    verify(mockClient, times(1)).update(download);
    assertEquals(Status.RUNNING, download.getStatus());
  }

  @Test
  void testMarkAsCancelled() {
    String downloadKey = "testKey";
    Download download = new Download();
    download.setStatus(Status.RUNNING);

    when(mockClient.get(downloadKey)).thenReturn(download);

    downloadUpdaterService.markAsCancelled(downloadKey);

    verify(mockClient, times(2)).get(downloadKey);
    verify(mockClient, times(1)).update(download);
    assertEquals(Status.CANCELLED, download.getStatus());
  }
}
