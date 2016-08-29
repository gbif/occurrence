package org.gbif.occurrence.download.license;

import org.gbif.api.vocabulary.License;

import com.google.common.base.Preconditions;

/**
 * Builder type that returns concrete implementation(s) of LicenseSelector.
 */
public class LicenseSelectors {

  /**
   * Return a LicenseSelector implementation that will collect all licenses and return the most restrictive one
   * based on the defaultLicense.
   * Note that null and non-concrete licenses are ignored.
   * @param defaultLicense the default (or base) license.
   * @return
   */
  public static LicenseSelector getMostRestrictiveLicenseSelector(final License defaultLicense){
    Preconditions.checkNotNull(defaultLicense, "LicenseSelector requires a default license");
    Preconditions.checkArgument(defaultLicense.isConcrete(), "LicenseSelector can only work on concrete license");
    return new LicenseSelector(){

      private License license = defaultLicense;

      @Override
      public void collectLicense(License license) {
        this.license = License.getMostRestrictive(defaultLicense, license, defaultLicense);
      }

      @Override
      public License getSelectedLicense() {
        return license;
      }
    };
  }
}
