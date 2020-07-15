package org.gbif.occurrence.test.mocks;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.service.CallbackService;

public class DownloadCallbackServiceMock implements CallbackService {

  private final OccurrenceDownloadService occurrenceDownloadService;

  public DownloadCallbackServiceMock(OccurrenceDownloadService occurrenceDownloadService) {
    this.occurrenceDownloadService = occurrenceDownloadService;
  }

  @Override
  public void processCallback(String jobId, String status) {
    Download download = occurrenceDownloadService.get(jobId);
    download.setStatus(Download.Status.valueOf(status));
    occurrenceDownloadService.update(download);
  }
}
