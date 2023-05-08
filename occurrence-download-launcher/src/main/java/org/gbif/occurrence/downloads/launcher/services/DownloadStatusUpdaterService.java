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

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.springframework.stereotype.Service;

/**
 * Service to be called from a SparkOutputListener to update a status of a download or called by
 * CRON to get list of unfinished downloads and check their status
 */
@Slf4j
@Service
public class DownloadStatusUpdaterService {

  private final OccurrenceDownloadClient occurrenceDownloadClient;

  public DownloadStatusUpdaterService(OccurrenceDownloadClient occurrenceDownloadClient) {
    this.occurrenceDownloadClient = occurrenceDownloadClient;
  }

  public List<Download> getExecutingDownloads() {
    return occurrenceDownloadClient
        .list(new PagingRequest(0, 20), Status.EXECUTING_STATUSES, null)
        .getResults();
  }

  public void updateStatus(String downloadKey, Status status) {
    Download download = occurrenceDownloadClient.get(downloadKey);
    if (download != null) {
      if (!status.equals(download.getStatus())) {
        log.info(
            "Update status for jobId {}, from {} to {}", downloadKey, download.getStatus(), status);
        download.setStatus(status);
        updateDownload(download);
      } else {
        log.debug(
            "Skiping downloads status updating for download {}, status is already {}",
            downloadKey,
            status);
      }
    } else {
      log.error("Can't update status for download {} to {}", downloadKey, status);
    }
  }

  public void updateDownload(Download download) {
    occurrenceDownloadClient.update(download);
  }
}
