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

import java.io.Serializable;

import org.apache.spark.sql.api.java.UDF3;
import org.locationtech.spatial4j.context.jts.DatelineRule;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;

import lombok.SneakyThrows;

public class ContainsUdf implements UDF3<String, Double, Double, Boolean> {

  public static class WktReaderWrapper implements Serializable {

    private final WKTReader wktReader;

    private final JtsShapeFactory shapeFactory;

    public WktReaderWrapper() {
      JtsSpatialContextFactory contextFactory = createJtsSpatialContextFactory();
      wktReader = new WKTReader(contextFactory.newSpatialContext(), contextFactory);
      shapeFactory = new JtsShapeFactory(contextFactory.newSpatialContext(), contextFactory);
    }

    private JtsSpatialContextFactory createJtsSpatialContextFactory() {
      JtsSpatialContextFactory spatialContextFactory = new JtsSpatialContextFactory();
      spatialContextFactory.normWrapLongitude = true;
      spatialContextFactory.srid = 4326;
      // NB the “Rect” here: large polygons with ≥180° between longitudinal points will be processed incorrectly.
      spatialContextFactory.datelineRule = DatelineRule.ccwRect;
      return spatialContextFactory;
    }

    @SneakyThrows
    public Shape parse(String geometryAsWKT) {
      return wktReader.parse(geometryAsWKT);
    }

    public Point pointXY(Double longitude, Double latitude) {
      return shapeFactory.pointXY(longitude, latitude);
    }

  }

  private final static WktReaderWrapper WKT_READER_WRAPPER = new WktReaderWrapper();

  @Override
  @SneakyThrows
  public Boolean call(String geometryAsWKT, Double latitude, Double longitude) throws Exception {

    // sanitize the input
    if (geometryAsWKT == null
        || latitude == null
        || longitude == null
        || latitude > 90
        || latitude < -90
        || longitude > 180
        || longitude < -180) {
      return false;
    }

    Shape geom = WKT_READER_WRAPPER.parse(geometryAsWKT);

    // support any geometry - up to the user to make a sensible query
    Point point = WKT_READER_WRAPPER.pointXY(longitude, latitude);

    return geom.relate(point).intersects();
  }
}
