package org.gbif.occurrence.downloads.launcher.services.launcher.oozie;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadType;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.common.messaging.api.messages.DownloadLauncherMessage;
import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.gbif.occurrence.downloads.launcher.services.launcher.DownloadLauncher;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class OozieDownloadLauncherServiceTest {

  private OozieClient oozieClient;
  private OozieDownloadLauncherService oozieDownloadLauncherService;

  @Before
  public void setUp() {
    oozieClient = Mockito.mock(OozieClient.class);
    oozieDownloadLauncherService =
        new OozieDownloadLauncherService(
            oozieClient, new HashMap<>(), Mockito.mock(LockerService.class));
  }

  @Test
  public void testCreate() throws OozieClientException {

    PredicateDownloadRequest request =
        new PredicateDownloadRequest(
            null,
            "creator",
            Collections.singletonList("notificationAddresses"),
            false,
            DownloadFormat.DWCA,
            DownloadType.OCCURRENCE,
            Collections.emptySet());

    DownloadLauncherMessage message =
        new DownloadLauncherMessage(UUID.randomUUID().toString(), request);

    oozieDownloadLauncherService.create(message);

    verify(oozieClient).run(any());
  }

  @Test
  public void testCreateException() throws OozieClientException {
    PredicateDownloadRequest request =
        new PredicateDownloadRequest(
            null,
            "creator",
            Collections.singletonList("notificationAddresses"),
            false,
            DownloadFormat.DWCA,
            DownloadType.OCCURRENCE,
            Collections.emptySet());

    DownloadLauncherMessage message =
        new DownloadLauncherMessage(UUID.randomUUID().toString(), request);

    doThrow(OozieClientException.class).when(oozieClient).run(any());

    assertEquals(DownloadLauncher.JobStatus.FAILED, oozieDownloadLauncherService.create(message));
  }

  @Test
  public void testCancel() throws OozieClientException {
    String downloadKey = UUID.randomUUID().toString();

    when(oozieClient.getJobId(downloadKey)).thenReturn("jobId");

    assertEquals(
        DownloadLauncher.JobStatus.CANCELLED, oozieDownloadLauncherService.cancel(downloadKey));

    verify(oozieClient).kill(any());
  }

  @Test
  public void testCancelException() throws OozieClientException {
    String downloadKey = UUID.randomUUID().toString();

    when(oozieClient.getJobId(downloadKey)).thenReturn("jobId");

    doThrow(OozieClientException.class).when(oozieClient).kill(any());

    assertEquals(
        DownloadLauncher.JobStatus.FAILED, oozieDownloadLauncherService.cancel(downloadKey));
  }

  @Test
  public void testRenewRunningDownloadsStatuses() throws OozieClientException {
    Download download = new Download();
    download.setKey(UUID.randomUUID().toString());

    when(oozieClient.getJobId(download.getKey())).thenReturn("jobId");
    when(oozieClient.getStatus("jobId")).thenReturn(Status.RUNNING.name());

    oozieDownloadLauncherService.renewRunningDownloadsStatuses(Collections.singletonList(download));

    verify(oozieClient).getStatus(any());
  }
}
