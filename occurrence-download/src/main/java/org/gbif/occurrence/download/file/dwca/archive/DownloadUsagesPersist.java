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
import org.gbif.api.vocabulary.License;
import org.gbif.occurrence.download.file.DownloadJobConfiguration;

import java.util.Map;
import java.util.UUID;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
public class DownloadUsagesPersist {

  private final DownloadJobConfiguration configuration;
  private final OccurrenceDownloadService occurrenceDownloadService;


  public void persistUsages(Map<UUID,Long> datasetUsages) {
    // small downloads persist dataset usages while builds the citations file
    if (!configuration.isSmallDownload()) {
      try {
        occurrenceDownloadService.createUsages(configuration.getDownloadKey(), datasetUsages);
      } catch(Exception e) {
        log.error("Error persisting dataset usage information, downloadKey: {} for large download", configuration.getDownloadKey(), e);
      }
    }
  }

  /**
   * Persist download license that was assigned to the occurrence download.
   */
  public void persistDownloadLicense(Download download, License license) {
    try {
      download.setLicense(license);
      occurrenceDownloadService.update(download);
    } catch (Exception ex) {
      log.error("Error updating download license, downloadKey: {}, license: {}", configuration.getDownloadKey(), license, ex);
    }
  }
}
