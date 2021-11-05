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
package org.gbif.occurrence.hive.udf;

import java.text.ParseException;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.locationtech.spatial4j.context.jts.DatelineRule;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 * A simple UDF for Hive to test if a coordinate is contained in a geometry given as well known text.
 * Any geometry supported by Spatial4J / JTS is accepted, but it's up to the user to make sensible queries.
 * In particular polygons should have a complete exterior ring.
 *
 * Spatial4J cannot handle polygons with longitudinal points ≥ 180° apart.  The Spatial4J developer writes “Spatial4j
 * does it this way because it seemed a real-world polygon wouldn't have longitudinal points ≥ 180 degrees apart”
 * https://github.com/locationtech/spatial4j/issues/46
 *
 * Therefore, such polygons (e.g a rough polygon of the Pacific Ocean) should have interpolated points added by the
 * client.
 */
/*
 * Interpolating here would be rather difficult — the WKTReader doesn't give access to the coordinates until the
 * polygon has been broken, and the alternative is parsing WKT or making difficult changes to Spatial4J.
 */
@Description(name = "contains", value = "_FUNC_(geom_wkt, latitude, longitude)")
public class ContainsUDF extends UDF {

  private static final Logger LOG = LoggerFactory.getLogger(ContainsUDF.class);

  private final WKTReader wktReader;
  private final Map<String, Shape> geometryCache = Maps.newHashMap();
  private final BooleanWritable isContained = new BooleanWritable();

  private final JtsShapeFactory shapeFactory;

  public ContainsUDF () {
    JtsSpatialContextFactory spatialContextFactory = new JtsSpatialContextFactory();
    spatialContextFactory.normWrapLongitude = true;
    spatialContextFactory.srid = 4326;
    // NB the “Rect” here: large polygons with ≥180° between longitudinal points will be processed incorrectly.
    spatialContextFactory.datelineRule = DatelineRule.ccwRect;

    wktReader = new org.locationtech.spatial4j.io.WKTReader(spatialContextFactory.newSpatialContext(), spatialContextFactory);

    shapeFactory = new JtsShapeFactory(spatialContextFactory.newSpatialContext(), spatialContextFactory);
  }

  public BooleanWritable evaluate(Text geometryAsWKT, Double latitude, Double longitude) {
    isContained.set(false);

    try {
      // sanitize the input
      if (geometryAsWKT == null || latitude == null || longitude == null || latitude > 90 || latitude < -90
        || longitude > 180 || longitude < -180) {
        return isContained;
      }
      String geoWKTStr = geometryAsWKT.toString();
      Shape geom = geometryCache.get(geoWKTStr);
      if (geom == null) {
        geom = wktReader.read(geoWKTStr);
        geometryCache.put(geoWKTStr, geom);
      }

      // support any geometry - up to the user to make a sensible query
      Point point = shapeFactory.pointXY(longitude, latitude);

      isContained.set(geom.relate(point).intersects());

    } catch (ParseException | InvalidShapeException e) {
      LOG.error("Invalid geometry received: {}", geometryAsWKT.toString(), e);
      throw new RuntimeException(e);
    } catch (Exception e) {
      LOG.error("Error applying UDF", e);
      throw new RuntimeException(e);
    }

    return isContained;
  }
}
