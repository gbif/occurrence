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
package org.gbif.occurrence.trino.udf;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import lombok.SneakyThrows;
import org.locationtech.spatial4j.context.jts.DatelineRule;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;

import java.util.HashMap;
import java.util.Map;
public class ContainsUdf {

  private final WKTReader wktReader;

  private final Map<String, Shape> geometryCache = new HashMap<>();

  private final JtsShapeFactory shapeFactory;

  public ContainsUdf() {
    JtsSpatialContextFactory spatialContextFactory = new JtsSpatialContextFactory();
    spatialContextFactory.normWrapLongitude = true;
    spatialContextFactory.srid = 4326;
    // NB the “Rect” here: large polygons with ≥180° between longitudinal points will be processed incorrectly.
    spatialContextFactory.datelineRule = DatelineRule.ccwRect;

    wktReader = new org.locationtech.spatial4j.io.WKTReader(spatialContextFactory.newSpatialContext(), spatialContextFactory);

    shapeFactory = new JtsShapeFactory(spatialContextFactory.newSpatialContext(), spatialContextFactory);
  }

  @SneakyThrows
  private Shape getShapeAndCache(String geometryAsWKT) {
    Shape geom = geometryCache.get(geometryAsWKT);
    if (geom == null) {
      geom = wktReader.read(geometryAsWKT);
      geometryCache.put(geometryAsWKT, geom);
    }
    return geom;
  }

  @SneakyThrows
  @ScalarFunction(value = "contains", deterministic = true)
  @Description("Is the coordinate in the WKT Geometry")
  @SqlType(StandardTypes.BOOLEAN)
  public boolean contains(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice geometryAsWKT, @SqlNullable @SqlType(StandardTypes.DOUBLE) Double latitude, @SqlNullable @SqlType(StandardTypes.DOUBLE) Double longitude) {
    // sanitize the input
    if (geometryAsWKT == null || latitude == null || longitude == null || latitude > 90 || latitude < -90
      || longitude > 180 || longitude < -180) {
      return false;
    }

    Shape geom = getShapeAndCache(geometryAsWKT.toStringUtf8());

    // support any geometry - up to the user to make a sensible query
    Point point = shapeFactory.pointXY(longitude, latitude);

    return geom.relate(point).intersects();
  }
}
