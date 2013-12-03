package org.gbif.occurrencestore.interpreters;

import org.gbif.common.parsers.LongPrecisionStatus;
import org.gbif.common.parsers.ParseResult;
import org.gbif.common.parsers.geospatial.GeospatialParseUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Static class for calculating a single depth value given min, max, and precision.
 */
public class DepthInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(DepthInterpreter.class);

  private DepthInterpreter() {
  }

  /**
   * Calculate a single depth value (in m) as representative for given min, max, and precision.
   *
   * @param min       minimum depth (in m)
   * @param max       maximum depth (in m)
   * @param precision min and max are precise to +/- this value (in m)
   *
   * @return a single representative depth (in m)
   */
  public static Integer interpretDepth(String min, String max, String precision) {
    if (min == null && max == null) return null;

    Long depth = null;
    ParseResult<LongPrecisionStatus> result = GeospatialParseUtils.parseDepth(min, max, precision);
    if (result.isSuccessful() && result.getPayload().getValue() != null) {
      depth = result.getPayload().getValue();
      depth = depth / 100l;
    }

    // long for depth doesn't make sense - max int metres is 2 million km
    Integer intDepth = null;
    if (depth != null) {
      if (depth > Integer.MAX_VALUE) {
        LOG
          .warn("Bad altitude value of [" + depth + "] - reducing to max integer value of [" + Integer.MAX_VALUE + "]");
        intDepth = Integer.MAX_VALUE;
      } else {
        intDepth = depth.intValue();
      }
    }

    return intDepth;
  }
}
