package org.gbif.occurrence.processor;

import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.LatLng;
import org.gbif.occurrence.processor.interpreting.util.Wgs84Projection;

/**
 *
 */
public class TryReprojection {
  public static void main (String[] args) {
    System.out.println("Try a reprojection");
    ParseResult<LatLng> x = Wgs84Projection.reproject(2, 2, "CAPE");
    System.out.println("Reprojection done: " + x);
  }
}
