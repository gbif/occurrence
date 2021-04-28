package org.gbif.occurrence.cli.regsitry.sync;

import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Installation;
import org.gbif.api.model.registry.Network;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.model.registry.eml.Project;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.License;
import org.gbif.occurrence.cli.registry.sync.RegistryBasedOccurrenceMutator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
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

  @Test
  public void testInstallationTitleUpdate() {

    Installation oldInstallation = new Installation();
    oldInstallation.setKey(UUID.randomUUID());
    oldInstallation.setTitle("InstTest");

    Installation newInstallation = new Installation();
    newInstallation.setKey(oldInstallation.getKey());
    newInstallation.setTitle("InstTestNew");

    //Test installation must be updated because its title changed
    assertTrue(OCC_MUTATOR.requiresUpdate(oldInstallation, newInstallation));

    //Title back to original value
    newInstallation.setTitle(oldInstallation.getTitle());
    assertFalse(OCC_MUTATOR.requiresUpdate(oldInstallation, newInstallation));
  }

  @Test
  public void testNetworkTitleUpdate() {

    Network oldNetwork = new Network();
    oldNetwork.setKey(UUID.randomUUID());
    oldNetwork.setTitle("NetTest");

    Network newNetwork = new Network();
    newNetwork.setKey(oldNetwork.getKey());
    newNetwork.setTitle("NetTestNew");

    //Test installation must be updated because its title changed
    assertTrue(OCC_MUTATOR.requiresUpdate(oldNetwork, newNetwork));

    //Title back to original value
    newNetwork.setTitle(oldNetwork.getTitle());
    assertFalse(OCC_MUTATOR.requiresUpdate(oldNetwork, newNetwork));
  }

  @Test
  public void testDatasetNetworkKeysUpdate() {

    List<UUID> networkKeys = Arrays.asList(UUID.randomUUID(), UUID.randomUUID());

    Dataset oldDataset = new Dataset();
    oldDataset.setKey(UUID.randomUUID());
    oldDataset.setNetworkKeys(networkKeys);


    Dataset newDataset = new Dataset();
    newDataset.setKey(oldDataset.getKey());
    List<UUID> reversedKeys = new ArrayList<>(networkKeys);
    Collections.reverse(reversedKeys);
    newDataset.setNetworkKeys(reversedKeys);

    //Test that changing the order of network keys doesn't affect
    assertFalse(OCC_MUTATOR.requiresUpdate(oldDataset, newDataset));

    //Network key removed
    reversedKeys.remove(0);
    newDataset.setNetworkKeys(reversedKeys);
    assertTrue(OCC_MUTATOR.requiresUpdate(oldDataset, newDataset));


    //Null checks
    newDataset.setNetworkKeys(null);
    assertTrue(OCC_MUTATOR.requiresUpdate(oldDataset, newDataset));


    //Null checks
    newDataset.setNetworkKeys(networkKeys);
    oldDataset.setNetworkKeys(null);
    assertTrue(OCC_MUTATOR.requiresUpdate(oldDataset, newDataset));
  }

  @Test
  public void testDatasetTitleUpdate() {

    Dataset oldDataset = new Dataset();
    oldDataset.setKey(UUID.randomUUID());
    oldDataset.setTitle("DatasetT");


    Dataset newDataset = new Dataset();
    newDataset.setKey(oldDataset.getKey());
    newDataset.setTitle(oldDataset.getTitle());

    //No change
    assertFalse(OCC_MUTATOR.requiresUpdate(oldDataset, newDataset));

    //Title changed
    newDataset.setTitle("DatasetTT");
    assertTrue(OCC_MUTATOR.requiresUpdate(oldDataset, newDataset));

    //Null checks
    newDataset.setTitle(null);
    assertTrue(OCC_MUTATOR.requiresUpdate(oldDataset, newDataset));

    //Null checks
    newDataset.setTitle("DatasetT");
    oldDataset.setTitle(null);
    assertTrue(OCC_MUTATOR.requiresUpdate(oldDataset, newDataset));
  }

  @Test
  public void testDatasetProjectUpdate() {

    Dataset oldDataset = new Dataset();
    oldDataset.setKey(UUID.randomUUID());
    oldDataset.setTitle("DatasetT");

    Project oldProject = new Project();
    oldProject.setIdentifier("1");
    oldDataset.setProject(oldProject);


    Dataset newDataset = new Dataset();
    newDataset.setKey(oldDataset.getKey());
    newDataset.setTitle(oldDataset.getTitle());
    Project newProject = new Project();
    newProject.setIdentifier(oldProject.getIdentifier());
    newDataset.setProject(newProject);

    //No change
    assertFalse(OCC_MUTATOR.requiresUpdate(oldDataset, newDataset));

    //Identifier changed
    newProject.setIdentifier("2");
    assertTrue(OCC_MUTATOR.requiresUpdate(oldDataset, newDataset));

    //Null checks
    newDataset.setProject(null);
    assertTrue(OCC_MUTATOR.requiresUpdate(oldDataset, newDataset));

    //Null checks
    newDataset.setProject(newProject);
    oldDataset.setProject(null);
    assertTrue(OCC_MUTATOR.requiresUpdate(oldDataset, newDataset));
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
