package org.gbif.occurrence.cli.regsitry.sync;

import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.License;
import org.gbif.occurrence.cli.registry.sync.RegistryBasedOccurrenceMutator;
import org.gbif.occurrence.cli.registry.sync.SyncCommon;

import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link RegistryBasedOccurrenceMutator}
 */
public class RegistryBasedOccurrenceMutatorTest {

  private static RegistryBasedOccurrenceMutator OCC_MUTATOR = new RegistryBasedOccurrenceMutator();

  @Test
  public void testHBaseResultRequiresUpdate() {

    Organization mockPublishingOrg = getMockOrganization();
    Country publishingCountry = Country.ANTARCTICA;
    mockPublishingOrg.setCountry(publishingCountry);
    License license = License.CC0_1_0;

    Result hbaseValues = mockHBaseResult(mockPublishingOrg.getKey().toString(), publishingCountry, license);

    Dataset dataset = getMockDataset();
    dataset.setPublishingOrganizationKey(mockPublishingOrg.getKey());
    dataset.setLicense(license);
    assertFalse(OCC_MUTATOR.requiresUpdate(dataset, mockPublishingOrg, hbaseValues));

    //now change the license
    dataset.setLicense(License.CC_BY_4_0);
    assertTrue(OCC_MUTATOR.requiresUpdate(dataset, mockPublishingOrg, hbaseValues));
  }

  /**
   * Build a mock HBase {@link Result} that simulates what would be extracted from the occurrence-persistence module.
   *
   * @param publishingOrgKey
   * @param publishingCountry
   * @param license
   * @return
   */
  private Result mockHBaseResult(String publishingOrgKey, Country publishingCountry, License license){
    Result hbaseValues = Mockito.mock(Result.class);
    when(hbaseValues.getValue(SyncCommon.OCC_CF, SyncCommon.OOK_COL)).thenReturn(Bytes.toBytes(publishingOrgKey));
    when(hbaseValues.getValue(SyncCommon.OCC_CF, SyncCommon.HC_COL)).thenReturn(Bytes.toBytes(publishingCountry.getIso2LetterCode()));

    //Enums are extracted using ExtResultReader which uses the deprecated methods.
    KeyValue rawValue = Mockito.mock(KeyValue.class);
    when(rawValue.getValue()).thenReturn(Bytes.toBytes(License.CC0_1_0.name()));
    when(hbaseValues.getColumnLatest(SyncCommon.OCC_CF, SyncCommon.LICENSE_COL)).thenReturn(rawValue);
    return hbaseValues;
  }

  @Test
  public void testRequiresUpdateDataset() {

    Organization org = getMockOrganization();
    Dataset datasetBefore = getMockDataset();
    datasetBefore.setPublishingOrganizationKey(org.getKey());
    datasetBefore.setLicense(License.UNSPECIFIED);

    Dataset datasetAfter = new Dataset();
    datasetAfter.setKey(datasetBefore.getKey());
    datasetAfter.setPublishingOrganizationKey(org.getKey());
    datasetAfter.setLicense(License.CC0_1_0);

    assertTrue(OCC_MUTATOR.requiresUpdate(datasetBefore, datasetAfter));
    assertTrue(StringUtils.isNotBlank(OCC_MUTATOR.generateUpdateMessage(org, org, datasetBefore, datasetAfter)
            .orElse("")));
  }

  @Test
  public void testRequiresUpdateDatasetOrganization() {

    UUID randomUUID = UUID.randomUUID();
    Organization orgBefore = new Organization();
    orgBefore.setKey(randomUUID);
    orgBefore.setEndorsementApproved(true);
    orgBefore.setCountry(Country.ANDORRA);

    Organization orgAfter = new Organization();
    orgAfter.setKey(randomUUID);
    orgAfter.setEndorsementApproved(true);
    orgAfter.setCountry(Country.ANGOLA);

    assertTrue(OCC_MUTATOR.requiresUpdate(orgBefore, orgAfter));

    Dataset dataset = getMockDataset();
    dataset.setPublishingOrganizationKey(orgBefore.getKey());
    dataset.setLicense(License.CC0_1_0);
    assertTrue(StringUtils.isNotBlank(OCC_MUTATOR.generateUpdateMessage(orgBefore, orgAfter, dataset, dataset)
            .orElse("")));
  }

  private Organization getMockOrganization() {
    Organization org = new Organization();
    org.setKey(UUID.randomUUID());
    return org;
  }

  private Dataset getMockDataset() {
    Dataset dataset = new Dataset();
    dataset.setKey(UUID.randomUUID());
    return dataset;
  }
}
