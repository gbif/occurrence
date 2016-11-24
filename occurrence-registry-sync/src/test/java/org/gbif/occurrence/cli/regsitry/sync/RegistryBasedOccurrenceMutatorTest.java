package org.gbif.occurrence.cli.regsitry.sync;

import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.License;
import org.gbif.occurrence.cli.registry.sync.RegistryBasedOccurrenceMutator;

import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link RegistryBasedOccurrenceMutator}
 */
public class RegistryBasedOccurrenceMutatorTest {

  @Test
  public void testRequiresUpdateDataset() {
    RegistryBasedOccurrenceMutator mutator = new RegistryBasedOccurrenceMutator();

    Organization org = getMockOrganization();
    Dataset datasetBefore = getMockDataset();
    datasetBefore.setPublishingOrganizationKey(org.getKey());
    datasetBefore.setLicense(License.UNSPECIFIED);

    Dataset datasetAfter = new Dataset();
    datasetAfter.setKey(datasetBefore.getKey());
    datasetAfter.setPublishingOrganizationKey(org.getKey());
    datasetAfter.setLicense(License.CC0_1_0);

    assertTrue(mutator.requiresUpdate(datasetBefore, datasetAfter));
    assertTrue(StringUtils.isNotBlank(mutator.generateUpdateMessage(org, org, datasetBefore, datasetAfter)
            .orElse("")));
  }

  @Test
  public void testRequiresUpdateDatasetOrganization() {
    RegistryBasedOccurrenceMutator mutator = new RegistryBasedOccurrenceMutator();

    UUID randomUUID = UUID.randomUUID();
    Organization orgBefore = new Organization();
    orgBefore.setKey(randomUUID);
    orgBefore.setEndorsementApproved(true);
    orgBefore.setCountry(Country.ANDORRA);

    Organization orgAfter = new Organization();
    orgAfter.setKey(randomUUID);
    orgAfter.setEndorsementApproved(true);
    orgAfter.setCountry(Country.ANGOLA);

    assertTrue(mutator.requiresUpdate(orgBefore, orgAfter));

    Dataset dataset = getMockDataset();
    dataset.setPublishingOrganizationKey(orgBefore.getKey());
    dataset.setLicense(License.CC0_1_0);
    assertTrue(StringUtils.isNotBlank(mutator.generateUpdateMessage(orgBefore, orgAfter, dataset, dataset)
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
