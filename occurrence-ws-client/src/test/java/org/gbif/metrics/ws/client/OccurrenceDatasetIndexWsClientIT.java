package org.gbif.metrics.ws.client;

import org.gbif.api.service.occurrence.OccurrenceDatasetIndexService;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * A simple Grizzly based WS IT test
 */
@Ignore
// TODO: test failing for incompatible javax version?
public class OccurrenceDatasetIndexWsClientIT {

  // should this change then we are really in trouble
  private static final int ANIMALIA_KEY = 1;
  private OccurrenceDatasetIndexService occurrenceDatasetIndexService; // under test

  @Autowired
  public OccurrenceDatasetIndexWsClientIT(OccurrenceDatasetIndexService occurrenceDatasetIndexService) {
    this.occurrenceDatasetIndexService = occurrenceDatasetIndexService;
  }

  /**
   * Ensures that the read works without throwing exception.
   */
  @Test
  public void basicLookup() {
    occurrenceDatasetIndexService.occurrenceDatasetsForNubKey(ANIMALIA_KEY);
  }

}
