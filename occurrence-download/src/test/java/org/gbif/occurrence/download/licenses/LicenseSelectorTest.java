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
package org.gbif.occurrence.download.licenses;

import org.gbif.api.vocabulary.License;
import org.gbif.occurrence.download.license.LicenseSelector;
import org.gbif.occurrence.download.license.LicenseSelectors;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for license selection .
 */
public class LicenseSelectorTest {

  @Test
  public void testLicenseSelectorByBase() {
    LicenseSelector licenseSelector = LicenseSelectors.getMostRestrictiveLicenseSelector(License.CC_BY_4_0);
    licenseSelector.collectLicense(License.CC0_1_0);
    assertEquals(License.CC_BY_4_0, licenseSelector.getSelectedLicense());
  }

  @Test
  public void testLicenseSelectorCC0Base() {
    LicenseSelector licenseSelector = LicenseSelectors.getMostRestrictiveLicenseSelector(License.CC0_1_0);
    licenseSelector.collectLicense(License.CC0_1_0);
    licenseSelector.collectLicense(License.UNSUPPORTED);
    licenseSelector.collectLicense(null);
    assertEquals(License.CC0_1_0, licenseSelector.getSelectedLicense());
  }

  @Test
  public void testLicenseSelectorCC0Base2() {
    LicenseSelector licenseSelector = LicenseSelectors.getMostRestrictiveLicenseSelector(License.CC0_1_0);
    licenseSelector.collectLicense(License.CC0_1_0);
    licenseSelector.collectLicense(License.CC_BY_NC_4_0);
    assertEquals(License.CC_BY_NC_4_0, licenseSelector.getSelectedLicense());
  }

  @Test
  public void testLicenseSelectorCCBY4ToCCBYNC() {
    LicenseSelector licenseSelector = LicenseSelectors.getMostRestrictiveLicenseSelector(License.CC_BY_4_0);
    licenseSelector.collectLicense(License.CC_BY_4_0);
    licenseSelector.collectLicense(License.CC_BY_NC_4_0);
    licenseSelector.collectLicense(License.CC0_1_0);
    assertEquals(License.CC_BY_NC_4_0, licenseSelector.getSelectedLicense());
  }
}
