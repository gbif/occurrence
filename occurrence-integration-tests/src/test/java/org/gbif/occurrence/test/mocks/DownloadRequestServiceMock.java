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
package org.gbif.occurrence.test.mocks;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.service.CallbackService;

import java.io.File;
import java.io.InputStream;
import java.util.Date;
import java.util.Objects;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;

import lombok.SneakyThrows;

/**
 * Download request service that uses directly the Callback service and mock OccurrenceDownloadService.
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
  public String create(@NotNull DownloadRequest downloadRequest, String source) {
    Download download = new Download();
    download.setStatus(Download.Status.PREPARING);
    download.setRequest(downloadRequest);
    download.setCreated(new Date());
    download.setSource(source);
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

  @Nullable
  @Override
  @SneakyThrows
  public File getResultFile(Download download) {
    return resourceLoader.getResource(TEST_FILE).getFile();
  }
}
