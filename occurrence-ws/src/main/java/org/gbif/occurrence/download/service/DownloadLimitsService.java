package org.gbif.occurrence.download.service;

import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.service.conf.DownloadLimits;

import javax.inject.Inject;

/**
 * Helper service that checks if a download request should be accepted under the allowed limits.
 */
public class DownloadLimitsService {

  private final OccurrenceDownloadService occurrenceDownloadService;
  private final DownloadLimits downloadLimits;

  @Inject
  public DownloadLimitsService(OccurrenceDownloadService occurrenceDownloadService, DownloadLimits downloadLimits) {
    this.occurrenceDownloadService = occurrenceDownloadService;
    this.downloadLimits = downloadLimits;
  }

  /**
   * Checks if the user is allowed to create a download request.
   * Validates if the download is under the limits of simultaneous downloads.
   */
  public boolean isInDownloadLimits(String userName) {
    PagingResponse<Download> userDownloads = occurrenceDownloadService.listByUser(userName,
                                                        new PagingRequest(0, 0),
                                                        Download.Status.EXECUTING_STATUSES);
    if (userDownloads.getCount() >= downloadLimits.getMaxUserDownloads()) {
      return false;
    } else {
      PagingResponse<Download> executingDownloads = occurrenceDownloadService.list(new PagingRequest(0, 0),
                                                                                    Download.Status.EXECUTING_STATUSES);
      return !downloadLimits.violatesLimits(userDownloads.getCount().intValue(),
                                            executingDownloads.getCount().intValue());
    }
  }
}
