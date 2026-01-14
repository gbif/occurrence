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
package org.gbif.occurrence.download.file.dwca.archive;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.service.registry.OccurrenceDownloadService;

import java.util.Map;
import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.gbif.api.vocabulary.License;

@Slf4j
@AllArgsConstructor(staticName = "create")
public class DownloadUsagesPersist {

  private final OccurrenceDownloadService occurrenceDownloadService;

  public void persistUsages(String downloadKey, Map<UUID,Long> datasetUsages) {
    try {
      log.debug("Create usage for download key: {}", downloadKey);
      occurrenceDownloadService.createUsages(downloadKey, datasetUsages);
    } catch(Exception e) {
      log.error("Error persisting dataset usage information, downloadKey: {} for large download", downloadKey, e);
    }
  }

  /**
   * Persist upadated download.
   */
  public void persistDownloadLicenseAndTotalRecords(String downloadKey, License license, long totalRecords) {
    try {
      log.info("Persist download {} with license {} and total records {}", downloadKey, license, totalRecords);
      occurrenceDownloadService.updateLicenseAndTotalRecords(downloadKey, license, totalRecords);
    } catch (Exception ex) {
      log.error("Error updating download license and total records, downloadKey: {}", downloadKey, ex);
    }
  }
}
