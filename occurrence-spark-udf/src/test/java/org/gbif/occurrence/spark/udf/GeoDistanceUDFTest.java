/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.spark.udf;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class GeoDistanceUDFTest {

  @Test
  public void evaluateTest() throws Exception {

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

  private boolean contains(Double latitude1, Double longitude1, String distance, Double latitude2, Double longitude2) throws Exception {
    return new GeoDistanceUdf().call(latitude1, longitude1, distance, latitude2, longitude2);
  }
}
