package org.gbif.occurrence.hive.udf;

import org.gbif.api.model.occurrence.geo.DistanceUnit;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.lucene.util.SloppyMath;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple UDF for Hive to test if a coordinate is within a given distance of a geopoint.
 */
@Description(name = "geo_distance", value = "_FUNC_(centroidLatitude, centroidLongitude, distance, latitude, longitude)")
public class GeoDistanceUDF extends UDF {

  private static final Logger LOG = LoggerFactory.getLogger(GeoDistanceUDF.class);

  private final BooleanWritable isContained = new BooleanWritable();

  public BooleanWritable evaluate(Double latitudeCentroid, Double longitudeCentroid, String distance, Double latitude, Double longitude) {
    isContained.set(false);

    try {
      // sanitize the input
      if (latitudeCentroid == null || longitudeCentroid == null || distance == null || latitude == null || longitude == null || latitude > 90 || latitude < -90
        || longitude > 180 || longitude < -180) {
        return isContained;
      }
      DistanceUnit.GeoDistance geoDistance = new DistanceUnit.GeoDistance(latitudeCentroid,
                                                                          longitudeCentroid,
                                                                          DistanceUnit.parseDistance(distance));

      double haversinDistance = SloppyMath.haversinMeters(latitudeCentroid, longitudeCentroid, latitude, longitude);
      double distanceInMeters = DistanceUnit.convert(geoDistance.getDistance().getValue(),
                                                     geoDistance.getDistance().getUnit(),
                                                     DistanceUnit.METERS);

      isContained.set(haversinDistance <= distanceInMeters);

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
