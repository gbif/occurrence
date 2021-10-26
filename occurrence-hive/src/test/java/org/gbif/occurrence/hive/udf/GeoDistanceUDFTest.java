package org.gbif.occurrence.hive.udf;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GeoDistanceUDFTest {

  @Test
  public void evaluateTest() {

    // At the Equator, moving 0.04° east is 4448m.
    assertTrue(contains(0d, 0d, "5km", 0d, 0.04d));
    // And 0.5° east is 5560m.
    assertFalse(contains(0d, 0d, "5km", 0d, 0.05d));

    // At 80° north, moving 0.04° east is only 772m.
    assertTrue(contains(80d, 0d, "5km", 80d, 0.04d));
    // But we can move 0.25° east and still have moved only 4827m.
    assertTrue(contains(80d, 0d, "5km", 80d, 0.25d));
    // This is 5020m.
    assertFalse(contains(80d, 0d, "5km", 80d, 0.26d));

    // Moving north is similar at either latitude
    assertTrue(contains(0d, 0d, "5km", 0.04d, 0d));
    assertFalse(contains(0d, 0d, "5km", 0.05d, 0d));
    assertTrue(contains(80d, 0d, "5km", 80.04d, 0d));
    assertFalse(contains(80d, 0d, "5km", 80.05d, 0d));
  }

  private boolean contains(Double latitude1, Double longitude1, String distance, Double latitude2, Double longitude2) {
    return new GeoDistanceUDF().evaluate(latitude1, longitude1, distance, latitude2, longitude2).get();
  }
}
