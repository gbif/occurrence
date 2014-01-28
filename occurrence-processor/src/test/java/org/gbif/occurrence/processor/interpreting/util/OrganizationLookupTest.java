package org.gbif.occurrence.processor.interpreting.util;

import org.gbif.api.model.registry.Organization;
import org.gbif.api.vocabulary.Country;

import java.util.UUID;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Ignore("requires live webservice")
public class OrganizationLookupTest {

  private static final String BGBM_KEY = "57254bd0-8256-11d8-b7ed-b8a03c50a862";
  private static final String BOGART_DATASET_KEY = "85697f04-f762-11e1-a439-00145eb45e9a";

  @Test
  public void testOrgLookup(){
    Organization org = OrganizationLookup.getOrgByDataset(UUID.fromString(BOGART_DATASET_KEY));
    assertEquals(BGBM_KEY, org.getKey().toString());
  }

  @Test
  public void testCountryLookup() {
    Country result = OrganizationLookup.getOrgCountry(UUID.fromString(BGBM_KEY));
    assertEquals(Country.GERMANY, result);
  }

  @Test
  public void testBadCountryLookup() {
    Country result = OrganizationLookup.getOrgCountry(UUID.randomUUID());
    assertNull(result);
  }
}
