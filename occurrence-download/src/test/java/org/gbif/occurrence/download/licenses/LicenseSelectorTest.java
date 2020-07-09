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
