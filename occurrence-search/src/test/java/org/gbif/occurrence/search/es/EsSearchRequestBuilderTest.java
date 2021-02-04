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
