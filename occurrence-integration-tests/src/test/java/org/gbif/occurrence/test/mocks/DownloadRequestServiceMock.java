package org.gbif.occurrence.test.mocks;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.service.CallbackService;

import java.io.File;
import java.io.InputStream;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;

/**
 * Download request service that uses the directly the Callback service and mock OccurrenceDownloadService.
 * All file download request resolve to the same test file 0011066-200127171203522.zip.
 */
public class DownloadRequestServiceMock implements DownloadRequestService {

  private static final String TEST_FILE  = "classpath:0011066-200127171203522.zip";


  private final OccurrenceDownloadService occurrenceDownloadService;

  private final CallbackService downloadCallbackService;

  private final ResourceLoader resourceLoader;

  @Autowired
  public DownloadRequestServiceMock(OccurrenceDownloadService occurrenceDownloadService,
                                    CallbackService downloadCallbackService,
                                    ResourceLoader resourceLoader) {
    this.occurrenceDownloadService = occurrenceDownloadService;
    this.downloadCallbackService = downloadCallbackService;
    this.resourceLoader = resourceLoader;
  }

  @Override
  public void cancel(@NotNull String downloadKey) {
    Download download = occurrenceDownloadService.get(downloadKey);
    if (Objects.nonNull(download)) {
      downloadCallbackService.processCallback(download.getKey(), Download.Status.CANCELLED.name());
    }
  }

  @Override
  public String create(@NotNull DownloadRequest downloadRequest) {
    Download download = new Download();
    download.setStatus(Download.Status.PREPARING);
    download.setRequest(downloadRequest);
    occurrenceDownloadService.create(download);
    downloadCallbackService.processCallback(download.getKey(), Download.Status.PREPARING.name());
    return download.getKey();
  }

  @Nullable
  @Override
  @SneakyThrows
  public InputStream getResult(String downloadKey) {
    return resourceLoader.getResource(TEST_FILE).getInputStream();
  }

  @Nullable
  @Override
  @SneakyThrows
  public File getResultFile(String s) {
    return resourceLoader.getResource(TEST_FILE).getFile();
  }
}
