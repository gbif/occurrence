package org.gbif.occurrence.download.license;

import org.gbif.api.vocabulary.License;

/**
 * Interface specifying behavior to select a License.
 */
public interface LicenseSelector {

  /**
   * Collect license.
   * @param license
   */
  void collectLicense(License license);

  /**
   * Get the license selected according to the implementation.
   * @return
   */
  License getSelectedLicense();


}
