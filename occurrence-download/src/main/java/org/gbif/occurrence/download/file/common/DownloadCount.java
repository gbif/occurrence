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
package org.gbif.occurrence.download.file.common;

import lombok.extern.slf4j.Slf4j;
import org.gbif.api.service.registry.OccurrenceDownloadService;


/** Action for Species list download, helps with counts of the number of distinct species. */
@Slf4j
public class DownloadCount {


  private DownloadCount() {}

  /** Updates the species record count of the download entity. */
  public static void persistTotalRecords(
      String downloadKey, long recordCount, OccurrenceDownloadService occurrenceDownloadService) {
    try {
      if (downloadKey == null) {
        log.error("Download key can't be null");
      } else {
        log.info("Updating record count of download {}", downloadKey);
        occurrenceDownloadService.updateTotalRecords(downloadKey, recordCount);
      }
    } catch (Exception ex) {
      log.error(
          "Error updating record count for download workflow {}, reported count is {}",
          downloadKey,
          recordCount,
          ex);
    }
  }
}
