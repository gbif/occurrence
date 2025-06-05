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

import static org.gbif.api.model.occurrence.Download.Status.EXECUTING_STATUSES;
import static org.gbif.api.model.occurrence.Download.Status.FINISH_STATUSES;
import static org.gbif.api.model.occurrence.Download.Status.RUNNING;
import static org.gbif.api.model.occurrence.Download.Status.SUSPENDED;

import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.registry.ws.client.BaseDownloadClient;
import org.springframework.stereotype.Service;

/**
 * Service is to be called to update the status of a download or to work with the
 * OccurrenceDownloadClient.
 */
@Slf4j
public abstract class DownloadUpdaterService {

  private final BaseDownloadClient downloadClient;

  public DownloadUpdaterService(BaseDownloadClient downloadClient) {
    this.downloadClient = downloadClient;
  }

  public boolean isStatusFinished(String downloadKey) {
    Download download = downloadClient.get(downloadKey);
    return FINISH_STATUSES.contains(download.getStatus());
  }

  public List<Download> getExecutingDownloads() {
    return downloadClient
        .list(new PagingRequest(0, 48), Set.of(RUNNING, SUSPENDED), null)
        .getResults();
  }

  public void updateStatus(String downloadKey, Status status) {
    Download download = downloadClient.get(downloadKey);
    if (download != null) {
      if (!status.equals(download.getStatus())) {
        log.info(
            "Update status for jobId {}, from {} to {}", downloadKey, download.getStatus(), status);
        download.setStatus(status);
        updateDownload(download);
      } else {
        log.debug(
            "Skipping downloads status updating for download {}, status is already {}",
            downloadKey,
            status);
      }
    } else {
      log.error("Can't update status for download {} to {}", downloadKey, status);
    }
  }

  public void updateDownload(Download download) {
    downloadClient.update(download);
  }

  public void markAsCancelled(String downloadKey) {
    Download download = downloadClient.get(downloadKey);
    if (download != null && EXECUTING_STATUSES.contains(download.getStatus())) {
      updateStatus(downloadKey, Status.CANCELLED);
    }
  }
}
