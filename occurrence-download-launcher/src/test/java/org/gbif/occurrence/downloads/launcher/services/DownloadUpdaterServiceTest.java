package org.gbif.occurrence.downloads.launcher.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
