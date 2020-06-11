package org.gbif.metrics.ws.client;

import org.gbif.api.model.metrics.cube.OccurrenceCube;
import org.gbif.api.model.metrics.cube.ReadBuilder;
import org.gbif.api.service.metrics.CubeService;

import java.util.UUID;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.assertTrue;

/**
 * A simple Grizzly based WS IT test
 */
@Ignore("This is constantly dying in Jenkins with socket timeout - a grizzly/IT/Jenkins problem")
public class CubeWsClientIT {

  // should this change then we are really in trouble
  private static final int ANIMALIA_KEY = 1;
  private CubeService cubeService; // under test

  @Autowired
  public CubeWsClientIT(CubeService cubeService) {
    this.cubeService = cubeService;
  }

  /**
   * This simply does lookups to verify we can read something without error.
   * This is an IT of the total wiring, and not intended to check any business logic of the cube.
   */
  @Ignore("Tried to connect to table outside Grizzly that no longer existed throwing TableNotFoundException")
  public void basicLookup() {
    cubeService.get(new ReadBuilder().at(OccurrenceCube.TAXON_KEY, ANIMALIA_KEY));
    cubeService.get(new ReadBuilder().at(OccurrenceCube.DATASET_KEY, UUID.randomUUID()));
    cubeService.get(new ReadBuilder());
  }

  /**
   * An IT to simply check the scheme can be read, and that some rollups exist.
   * Is not meant to test any business logic.
   */
  @Test
  public void schema() {
    assertTrue("CubeIo schema says no rollups which can't be true",
               cubeService.getSchema().size() > 0);
  }
}
