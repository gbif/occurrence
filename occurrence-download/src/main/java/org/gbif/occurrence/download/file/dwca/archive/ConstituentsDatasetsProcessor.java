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

import org.gbif.api.model.registry.Dataset;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.vocabulary.License;
import org.gbif.occurrence.download.license.LicenseSelector;
import org.gbif.occurrence.download.license.LicenseSelectors;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConstituentsDatasetsProcessor implements Closeable, Consumer<ConstituentDataset> {

  private final LicenseSelector licenseSelector = LicenseSelectors.getMostRestrictiveLicenseSelector(License.CC0_1_0);
  private final ConstituentsRightsWriter rightsWriter;
  private final ConstituentsCitationWriter citationWriter;
  private final ConstituentsEmlWriter emlWriter;

  @Builder
  public ConstituentsDatasetsProcessor(DatasetService datasetService, File archiveDir) {
    rightsWriter = new ConstituentsRightsWriter(archiveDir);
    citationWriter = new ConstituentsCitationWriter(archiveDir);
    emlWriter = new ConstituentsEmlWriter(datasetService, archiveDir);
  }

  /**
   * Adds an eml file per dataset involved into a subfolder "dataset" which is supported by our dwc archive reader.
   * Create a rights.txt and citation.txt file targeted at humans to quickly yield an overview about rights and
   * datasets involved.
   * This method returns the License that must be assigned to the occurrence download file.
   */
  @Override
  public void accept(ConstituentDataset constituent) {
         log.info("Processing constituent dataset: {}", constituent.getKey());
        // catch errors for each uuid to make sure one broken dataset does not bring down the entire process
        try {
          Dataset dataset = constituent.getDataset();

          licenseSelector.collectLicense(dataset.getLicense());
          // citation
          citationWriter.accept(dataset);
          // rights
          rightsWriter.accept(dataset);
          // eml file
          emlWriter.accept(dataset);

          // add original author as content provider to main dataset description
          DwcaContactsUtil.getContentProviderContact(constituent.getDataset())
            .ifPresent(provider -> dataset.getContacts().add(provider));
        } catch (Exception e) {
          log.error("Error creating download file", e);
        }
  }

  @Override
  public void close() throws IOException {
    citationWriter.close();
    rightsWriter.close();
    emlWriter.close();
  }

  public License getSelectedLicense() {
    return licenseSelector.getSelectedLicense();
  }

}
