package org.gbif.occurrence.clustering;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class HaversineTest {

  @Test
  public void testDistance() {
    double distance = Haversine.distance(21.8656, -102.909, 21.86558d, -102.90929d);
    assertTrue(distance < 0.2, "Distance exceeds 200m");

    distance = Haversine.distance(21.506, -103.092, 21.50599, 	-103.09193);
    assertTrue(distance < 0.2, "Distance exceeds 200m");
  }
}
