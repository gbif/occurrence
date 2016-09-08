package org.gbif.occurrence.cli.regsitry.sync;

import org.gbif.api.model.registry.Dataset;
import org.gbif.api.vocabulary.License;
import org.gbif.occurrence.cli.registry.sync.RegistryBasedOccurrenceMutator;

import java.util.UUID;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class RegistryBasedOccurrenceMutatorTest {


  @Test
  public void testRequiresUpdateDataset() {
    RegistryBasedOccurrenceMutator mutator = new RegistryBasedOccurrenceMutator();

    UUID randomUUID = UUID.randomUUID();
    Dataset datasetBefore = new Dataset();
    datasetBefore.setPublishingOrganizationKey(randomUUID);
    datasetBefore.setLicense(License.UNSPECIFIED);

    Dataset datasetAfter = new Dataset();
    datasetAfter.setPublishingOrganizationKey(randomUUID);
    datasetAfter.setLicense(License.CC0_1_0);

    assertTrue(mutator.requiresUpdate(datasetBefore, datasetAfter));
  }
}
