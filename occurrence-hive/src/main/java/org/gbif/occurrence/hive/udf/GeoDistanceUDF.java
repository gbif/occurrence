package org.gbif.occurrence.hive.udf;

import org.gbif.api.model.occurrence.geo.DistanceUnit;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.lucene.util.SloppyMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple UDF for Hive to test if two coordinates are in a given distance.
 */
@Description(name = "geo_distance", value = "_FUNC_(centroidLatitude, centroidLongitude, distance, latitude, longitude)")
public class GeoDistanceUDF extends UDF {

  private static final Logger LOG = LoggerFactory.getLogger(GeoDistanceUDF.class);

  private final BooleanWritable isInDistance = new BooleanWritable();

  public BooleanWritable evaluate(Double lat1, Double lon1, String distance, Double lat2, Double lon2) {
    isInDistance.set(false);

    try {
      // sanitize the input
      if (lat1 == null || lon1 == null || distance == null || lat2 == null || lon2 == null || lat2 > 90 || lat2 < -90
        || lon2 > 180 || lon2 < -180) {
        return isInDistance;
      }

      isInDistance.set(areInHaversineDistance(lat1, lon1, distance, lat2, lon2));

    } catch (Exception e) {
      LOG.error("Error applying UDF", e);
      throw new RuntimeException(e);
    }
    return isInDistance;
  }

  /** Are 2 coordinates in haversine distance.*/
  private static boolean areInHaversineDistance(double lat1, double lon1, String distance, double lat2, double lon2) {
    return haversineDistance(lat1, lon1, lat2, lon2) <= distanceInMeters(distance);
  }

  /** Distance in meters, between 2  coordinates using the haversine method implemented by lucene */
  private static double haversineDistance(double lat1, double lon1, double lat2, double lon2) {
    return SloppyMath.haversinMeters(lat1, lon1, lat2, lon2);
  }

  /** Parse and converts a distance to meters.*/
  private static double distanceInMeters(String distance) {
    DistanceUnit.Distance geoDistance = DistanceUnit.parseDistance(distance);
    return DistanceUnit.convert(geoDistance.getValue(), geoDistance.getUnit(), DistanceUnit.METERS);
  }
}
