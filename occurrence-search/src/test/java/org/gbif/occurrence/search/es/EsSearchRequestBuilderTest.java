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
package org.gbif.occurrence.search.es;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/** Tests the {@link EsSearchRequestBuilder}. */
public class EsSearchRequestBuilderTest {

  @Test
  public void normalizePolygonCoordinatesTest() {
    Coordinate[] duplicateCoords =
        new Coordinate[] {
          new Coordinate(1.0, 3.0),
          new Coordinate(1.0, 3.0),
          new Coordinate(2.0, 2.0),
          new Coordinate(2.0, 4.0),
          new Coordinate(2.0, 4.0),
          new Coordinate(3.0, 1.0),
          new Coordinate(1.0, 3.0),
          new Coordinate(1.0, 3.0)
        };

    Coordinate[] expectedCoords =
        new Coordinate[] {
          new Coordinate(1.0, 3.0),
          new Coordinate(2.0, 2.0),
          new Coordinate(2.0, 4.0),
          new Coordinate(3.0, 1.0),
          new Coordinate(1.0, 3.0)
        };

    Coordinate[] normalizedCoords =
        EsSearchRequestBuilder.normalizePolygonCoordinates(duplicateCoords);

    assertArrayEquals(expectedCoords, normalizedCoords);
  }
}
