package org.gbif.occurrence.interpreters;

import org.gbif.common.parsers.LongPrecisionStatus;
import org.gbif.common.parsers.ParseResult;
import org.gbif.common.parsers.geospatial.GeospatialParseUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static class for calculating a single altitude value given min, max, and precision.
 */
public class AltitudeInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(AltitudeInterpreter.class);

  private AltitudeInterpreter() {
  }

  /**
   * Calculate a single altitude value (in m) as representative for given min, max, and precision.
   *
   * @param min       minimum altitude (in m)
   * @param max       maximum altitude (in m)
   * @param precision min and max are precise to +/- this value (in m)
   *
   * @return a single representative altitude (in m), or null if min and max are null
   */
  public static Integer interpretAltitude(String min, String max, String precision) {
    if (min == null && max == null) return null;

    Long altitude = null;
    ParseResult<LongPrecisionStatus> result = GeospatialParseUtils.parseAltitude(min, max, precision);
    if (result.isSuccessful() && result.getPayload().getValue() != null) {
      altitude = result.getPayload().getValue();
    }

    // long for altitude doesn't make sense - max int metres is 2 million km
    Integer intAlt = null;
    if (altitude != null) {
      if (altitude > Integer.MAX_VALUE) {
        LOG.warn(
          "Bad altitude value of [" + altitude + "] - reducing to max integer value of [" + Integer.MAX_VALUE + "]");
        intAlt = Integer.MAX_VALUE;
      } else {
        intAlt = altitude.intValue();
      }
    }

    return intAlt;
  }
}
