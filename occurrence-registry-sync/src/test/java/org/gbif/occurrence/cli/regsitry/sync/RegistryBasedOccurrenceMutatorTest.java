package org.gbif.occurrence.cli.regsitry.sync;

import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.License;
import org.gbif.occurrence.cli.registry.sync.RegistryBasedOccurrenceMutator;

import java.time.Instant;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link RegistryBasedOccurrenceMutator}
 */
public class RegistryBasedOccurrenceMutatorTest {

  private static RegistryBasedOccurrenceMutator OCC_MUTATOR = new RegistryBasedOccurrenceMutator();

  @Test
  public void testRequiresUpdateDataset() {

    Organization org = getMockOrganization();
    Dataset datasetBefore = getMockDataset();
    datasetBefore.setPublishingOrganizationKey(org.getKey());
    datasetBefore.setLicense(License.UNSPECIFIED);
    datasetBefore.setCreated(Date.from(Instant.now().minusSeconds(3600)));

    Dataset datasetAfter = new Dataset();
    datasetAfter.setKey(datasetBefore.getKey());
    datasetAfter.setPublishingOrganizationKey(org.getKey());
    datasetAfter.setLicense(License.CC0_1_0);
    datasetAfter.setCreated(datasetBefore.getCreated());

    assertTrue(OCC_MUTATOR.requiresUpdate(datasetBefore, datasetAfter));
    assertTrue(StringUtils.isNotBlank(OCC_MUTATOR.generateUpdateMessage(org, org, datasetBefore, datasetAfter)
      .orElse("")));
  }

  @Test
  public void testRequiresUpdateRecentDataset() {

    Organization org = getMockOrganization();
    Dataset datasetBefore = getMockDataset();
    datasetBefore.setPublishingOrganizationKey(org.getKey());
    datasetBefore.setLicense(License.UNSPECIFIED);
    datasetBefore.setCreated(Date.from(Instant.now().minusSeconds(60)));

    Dataset datasetAfter = new Dataset();
    datasetAfter.setKey(datasetBefore.getKey());
    datasetAfter.setPublishingOrganizationKey(org.getKey());
    datasetAfter.setLicense(License.CC0_1_0);
    datasetAfter.setCreated(datasetBefore.getCreated());

    assertFalse(OCC_MUTATOR.requiresUpdate(datasetBefore, datasetAfter));
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
