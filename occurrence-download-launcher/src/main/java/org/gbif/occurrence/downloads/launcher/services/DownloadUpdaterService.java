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

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.Download.Status;
import org.gbif.registry.ws.client.OccurrenceDownloadClient;
import org.springframework.stereotype.Service;

/**
 * Service is to be called to update the status of a download or to work with the
 * OccurrenceDownloadClient.
 */
@Slf4j
@Service
public class DownloadUpdaterService {

  private static final Retry RETRY =
      Retry.of(
          "occurrenceDownloadApiCall",
          RetryConfig.custom()
              .maxAttempts(7)
              .intervalFunction(IntervalFunction.ofExponentialBackoff(Duration.ofSeconds(6)))
              .build());

  private final OccurrenceDownloadClient occurrenceDownloadClient;

  public DownloadUpdaterService(OccurrenceDownloadClient occurrenceDownloadClient) {
    this.occurrenceDownloadClient = occurrenceDownloadClient;
  }

  public boolean isStatusFinished(String downloadKey) {
    Download download = getDownloadWithRetry(downloadKey);
    return FINISH_STATUSES.contains(download.getStatus());
  }

  public List<Download> getExecutingDownloads() {
    return Retry.decorateSupplier(
            RETRY,
            () ->
                occurrenceDownloadClient
                    .list(new PagingRequest(0, 48), Set.of(RUNNING, SUSPENDED), null)
                    .getResults())
        .get();
  }

  public void updateStatus(String downloadKey, Status status) {
    Download download = getDownloadWithRetry(downloadKey);
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
    Retry.decorateRunnable(RETRY, () -> occurrenceDownloadClient.update(download)).run();
  }

  public void markAsCancelled(String downloadKey) {
    Download download = getDownloadWithRetry(downloadKey);
    if (download != null && EXECUTING_STATUSES.contains(download.getStatus())) {
      updateStatus(downloadKey, Status.CANCELLED);
    }
  }

  private Download getDownloadWithRetry(String downloadKey) {
    return Retry.decorateSupplier(RETRY, () -> occurrenceDownloadClient.get(downloadKey)).get();
  }
}
