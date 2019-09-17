package org.gbif.occurrence.download.service;

import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
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
  public String exceedsSimultaneousDownloadLimit(String userName) {
    PagingResponse<Download> userDownloads = occurrenceDownloadService.listByUser(userName,
                                                        new PagingRequest(0, 0),
                                                        Download.Status.EXECUTING_STATUSES);
    if (userDownloads.getCount() >= downloadLimits.getMaxUserDownloads()) {
      return "User "+userName+" has too many simultaneous downloads; the limit is "+downloadLimits.getMaxUserDownloads()+".\n"
      + "Please wait for some to complete, or cancel any unwanted downloads.  See your user page.";
    }

    PagingResponse<Download> executingDownloads = occurrenceDownloadService.list(new PagingRequest(0, 0),
                                                                                 Download.Status.EXECUTING_STATUSES);

    if (downloadLimits.violatesLimits(userDownloads.getCount().intValue(), executingDownloads.getCount().intValue())) {
      return "Too many downloads are running.  Please wait for some to complete: see the GBIF health status page.";
    }

    return null;
  }

  /**
   * Checks if the user is allowed to create this download request.
   * Validates if the download is too long/complicated.
   */
  public String exceedsDownloadComplexity(DownloadRequest request) {

    if (request instanceof PredicateDownloadRequest) {
      return downloadLimits.violatesFilterRules(((PredicateDownloadRequest)request).getPredicate());
    }

    return null;
  }
}
