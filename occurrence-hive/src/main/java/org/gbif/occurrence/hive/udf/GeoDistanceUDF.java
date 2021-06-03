package org.gbif.occurrence.hive.udf;

import org.gbif.api.model.occurrence.geo.DistanceUnit;

import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BooleanWritable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.context.jts.DatelineRule;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple UDF for Hive to test if a coordinate is within a given distance of a geopoint.
 */
@Description(name = "geo_distance", value = "_FUNC_(centroidLatitude, centroidLongitude, distance, latitude, longitude)")
public class GeoDistanceUDF extends UDF {

  private static final Logger LOG = LoggerFactory.getLogger(GeoDistanceUDF.class);

  private final Map<String, Geometry> geometryCache = Maps.newHashMap();
  private final BooleanWritable isContained = new BooleanWritable();

  private final JtsShapeFactory shapeFactory;

  public GeoDistanceUDF() {
    JtsSpatialContextFactory spatialContextFactory = new JtsSpatialContextFactory();
    spatialContextFactory.normWrapLongitude = true;
    spatialContextFactory.srid = 4326;
    // NB the “Rect” here: large polygons with ≥180° between longitudinal points will be processed incorrectly.
    spatialContextFactory.datelineRule = DatelineRule.ccwRect;

    shapeFactory = new JtsShapeFactory(spatialContextFactory.newSpatialContext(), spatialContextFactory);
  }

  public BooleanWritable evaluate(Double latitudeCentroid, Double longitudeCentroid, String distance, Double latitude, Double longitude) {
    isContained.set(false);

    try {
      // sanitize the input
      if (latitudeCentroid == null || longitudeCentroid == null || distance == null || latitude == null || longitude == null || latitude > 90 || latitude < -90
        || longitude > 180 || longitude < -180) {
        return isContained;
      }
      DistanceUnit.GeoDistance geoDistance = new DistanceUnit.GeoDistance(latitudeCentroid, longitudeCentroid, DistanceUnit.parseDistance(distance));

      Geometry geom = geometryCache.get(geoDistance.toGeoDistanceString());
      if (geom == null) {
        geom = JtsSpatialContext.GEO.getShapeFactory().getGeometryFrom(
        shapeFactory.circle(longitudeCentroid, latitudeCentroid,
                            DistanceUtils.dist2Degrees(DistanceUnit.convert(geoDistance.getDistance().getValue(),
                                                                            geoDistance.getDistance().getUnit(), DistanceUnit.KILOMETERS),
                                                       DistanceUtils.EARTH_MEAN_RADIUS_KM)));
        geometryCache.put(geoDistance.toGeoDistanceString(), geom);
      }

      // support any geometry - up to the user to make a sensible query
      Geometry point = JtsSpatialContext.GEO.getShapeFactory().getGeometryFrom(shapeFactory.pointXY(longitude, latitude));

      isContained.set(geom.intersects(point));

    } catch (InvalidShapeException e) {
      LOG.error("Invalid geometry received", e);
      throw new RuntimeException(e);
    } catch (Exception e) {
      LOG.error("Error applying UDF", e);
      throw new RuntimeException(e);
    }

    return isContained;
  }
}
