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
   *
   * @param defaultLicense the default (or base) license.
   */
  public static LicenseSelector getMostRestrictiveLicenseSelector(final License defaultLicense) {
    Preconditions.checkNotNull(defaultLicense, "LicenseSelector requires a default license");
    Preconditions.checkArgument(defaultLicense.isConcrete(), "LicenseSelector can only work on concrete license");

    return new LicenseSelector() {

      private License license = defaultLicense;

      @Override
      public void collectLicense(License license) {
        this.license = License.getMostRestrictive(this.license, license, defaultLicense);
      }

      @Override
      public License getSelectedLicense() {
        return license;
      }
    };
  }
}
