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

import org.gbif.api.model.occurrence.geo.DistanceUnit;

import org.apache.lucene.util.SloppyMath;
import org.apache.spark.sql.api.java.UDF5;

import lombok.SneakyThrows;

/**
 * A simple UDF to test if two coordinates are within a given distance.
 */
public class GeoDistanceUdf implements UDF5<Double,Double,String,Double,Double,Boolean> {

  @Override
  @SneakyThrows
  public Boolean call(Double lat1, Double lon1, String distance, Double lat2, Double lon2) throws Exception {
    // sanitize the input
    if (lat1 == null || lon1 == null || distance == null || lat2 == null || lon2 == null || lat2 > 90 || lat2 < -90
      || lon2 > 180 || lon2 < -180) {
      return false;
    }
    return areInHaversineDistance(lat1, lon1, distance, lat2, lon2);
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
