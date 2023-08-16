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
package org.gbif.occurrence.download.service;

import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.service.conf.DownloadLimits;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Helper service that checks if a download request should be accepted under the allowed limits.
 */
@Component
public class DownloadLimitsService {

  private final OccurrenceDownloadService occurrenceDownloadService;
  private final DownloadLimits downloadLimits;

  @Autowired
  public DownloadLimitsService(OccurrenceDownloadService occurrenceDownloadService, DownloadLimits downloadLimits) {
    this.occurrenceDownloadService = occurrenceDownloadService;
    this.downloadLimits = downloadLimits;
  }

  /**
   * Checks if the user is allowed to create a download request.
   * Validates if the download is under the limits of simultaneous downloads.
   */
  public String exceedsSimultaneousDownloadLimit(String userName) {
    long userDownloadsCount = occurrenceDownloadService.countByUser(userName,
                                                        Download.Status.EXECUTING_STATUSES, null);
    if (userDownloadsCount >= downloadLimits.getMaxUserDownloads()) {
      return "User "+userName+" has too many simultaneous downloads; the limit is "+downloadLimits.getMaxUserDownloads()+".\n"
      + "Please wait for some to complete, or cancel any unwanted downloads.  See your user page.";
    }

    long executingDownloadsCount =
        occurrenceDownloadService.count(Download.Status.EXECUTING_STATUSES, null);

    if (downloadLimits.violatesLimits((int) userDownloadsCount, (int) executingDownloadsCount)) {
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
