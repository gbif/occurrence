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
package org.gbif.occurrence.download.citations;

import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.vocabulary.License;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.download.license.LicenseSelector;
import org.gbif.occurrence.download.license.LicenseSelectors;
import org.gbif.occurrence.download.util.RegistryClientUtil;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Persists a download's citations to the Registry.
 */
@Slf4j
@UtilityClass
public final class CitationsPersister extends CitationsFileReader {

  /**
   * Persists the dataset usage and license info into the Registry data base.
   */
  public static class PersistUsage implements BiConsumer<Map<UUID,Long>,Map<UUID,License>> {

    private final String downloadKey;
    private final LicenseSelector licenseSelector = LicenseSelectors.getMostRestrictiveLicenseSelector(License.CC0_1_0);
    private final OccurrenceDownloadService downloadService;

    public PersistUsage(String downloadKey, DwcTerm dwcTerm, String registryWsUrl, String registryUser, String registryPassword) {
      RegistryClientUtil registryClientUtil = new RegistryClientUtil(registryUser, registryPassword, registryWsUrl);
      this.downloadKey = downloadKey;
      this.downloadService = registryClientUtil.occurrenceDownloadService(dwcTerm);
    }

    private static Long sumCitations(Map<UUID, Long> citations) {
      return Optional.ofNullable(citations).map(c -> c.values().stream().reduce(0L, Long::sum)).orElse(0L);
    }

    @Override
    public void accept(Map<UUID, Long> datasetsCitation, Map<UUID, License> datasetLicenses) {
      if (datasetsCitation == null || datasetsCitation.isEmpty()) {
        log.info("No citation information to update as list of datasets is empty or null, hence ignoring the request");
      }

      try {
        datasetLicenses.values().forEach(licenseSelector::collectLicense);
        Long totalRecords = sumCitations(datasetsCitation);
        downloadService.updateLicenseAndTotalRecords(
            downloadKey, licenseSelector.getSelectedLicense(), totalRecords);
      } catch (Exception ex) {
        log.error("Error persisting download license information, downloadKey: {}, licenses: {} ",
          downloadKey, datasetLicenses.values(), ex);
      }

      try {
        log.debug("Create usage for download key: {}", downloadKey);
        downloadService.createUsages(downloadKey, datasetsCitation);
      } catch (Exception e) {
        log.error("Error persisting dataset usage information: {}", datasetsCitation, e);
      }
    }
  }
}
