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
import org.apache.lucene.util.SloppyMath;
import org.gbif.api.model.occurrence.geo.DistanceUnit;

public class GeoDistanceUdf {

  @SneakyThrows
  @ScalarFunction(value = "geoDistance", deterministic = true)
  @Description("Calculates the geo distance between two points")
  @SqlType(StandardTypes.BOOLEAN)
  public boolean geoDistance(@SqlNullable @SqlType(StandardTypes.DOUBLE) Double lat1,  @SqlNullable @SqlType(StandardTypes.DOUBLE) Double lon1,
                             @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice distance,
                             @SqlNullable @SqlType(StandardTypes.DOUBLE) Double lat2,  @SqlNullable @SqlType(StandardTypes.DOUBLE) Double lon2) {
    // sanitize the input
    if (lat1 == null || lon1 == null || distance == null || lat2 == null || lon2 == null || lat2 > 90 || lat2 < -90
      || lon2 > 180 || lon2 < -180) {
      return false;
    }
    return areInHaversineDistance(lat1, lon1, distance.toStringUtf8(), lat2, lon2);
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
