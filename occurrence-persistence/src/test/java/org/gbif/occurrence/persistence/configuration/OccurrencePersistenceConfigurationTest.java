package org.gbif.occurrence.persistence.configuration;

import org.gbif.api.service.occurrence.OccurrenceService;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.assertNotNull;

@Disabled("Not needed for pipelines")
public class OccurrencePersistenceConfigurationTest {

  private final OccurrenceService occurrenceService;

  @Autowired
  public OccurrencePersistenceConfigurationTest(OccurrenceService occurrenceService) {
    this.occurrenceService = occurrenceService;
  }

  // ensure that the service can be instantiated - if you change this, change the README to match!
  @Test
  public void testModule() {
    assertNotNull(occurrenceService);
  }
}
