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
package org.gbif.occurrence.trino.processor.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.DatumParser;
import org.gbif.common.parsers.geospatial.LatLng;
import org.geotools.factory.BasicFactories;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.cs.DefaultEllipsoidalCS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.DatumAuthorityFactory;
import org.opengis.referencing.datum.GeodeticDatum;
import org.opengis.referencing.operation.MathTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.Set;

/**
 * Utils class that reprojects to WGS84 based on geotools transformations and SRS databases.
 */
public class Wgs84Projection {

  private static final Logger LOG = LoggerFactory.getLogger(Wgs84Projection.class);
  private static final DatumParser PARSER = DatumParser.getInstance();
  private static final double SUSPICIOUS_SHIFT = 0.1d;
  private static DatumAuthorityFactory DATUM_FACTORY;

  static {
    try {
      DATUM_FACTORY = BasicFactories.getDefault().getDatumAuthorityFactory();
      LOG.info("Wgs84Projection utils created");
    } catch (Exception e) {
      LOG.error("Failed to create geotools datum factory", e);
    }
  }

  /**
   * Reproject the given coordinates into WGS84 coordinates based on a known source datum or SRS.
   * Darwin Core allows not only geodetic datums but also full spatial reference systems as values for "datum".
   * The method will always return lat lons even if the processing failed. In that case only issues are set and the
   * parsing result set to fail - but with a valid payload.
   *
   * @param lat   the original latitude
   * @param lon   the original longitude
   * @param datum the original geodetic datum the coordinates are in
   *
   * @return the reprojected coordinates or the original ones in case transformation failed
   */
  public static OccurrenceParseResult<LatLng> reproject(double lat, double lon, String datum) {
    Preconditions.checkArgument(lat >= -90d && lat <= 90d);
    Preconditions.checkArgument(lon >= -180d && lon <= 180d);

    Set<OccurrenceIssue> issues = EnumSet.noneOf(OccurrenceIssue.class);

    if (Strings.isNullOrEmpty(datum)) {
      issues.add(OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84);
      return OccurrenceParseResult.success(ParseResult.CONFIDENCE.DEFINITE, new LatLng(lat, lon), issues);
    }

    try {
      CoordinateReferenceSystem crs = parseCRS(datum);
      if (crs == null) {
        issues.add(OccurrenceIssue.GEODETIC_DATUM_INVALID);
        issues.add(OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84);

      } else {
        MathTransform transform = CRS.findMathTransform(crs, DefaultGeographicCRS.WGS84, true);
        // different CRS may swap the x/y axis for lat lon, so check first:
        double[] srcPt;
        double[] dstPt = new double[3];
        if (CRS.getAxisOrder(crs) == CRS.AxisOrder.NORTH_EAST) {
          // lat lon
          srcPt = new double[] {lat, lon, 0};
        } else {
          // lon lat
          LOG.debug("Use lon/lat ordering for reprojection with datum={} and lat/lon={}/{}", datum, lat, lon);
          srcPt = new double[] {lon, lat, 0};
        }

        transform.transform(srcPt, 0, dstPt, 0, 1);

        double lat2 = dstPt[1];
        double lon2 = dstPt[0];
        // verify the datum shift is reasonable
        if (Math.abs(lat - lat2) > SUSPICIOUS_SHIFT || Math.abs(lon - lon2) > SUSPICIOUS_SHIFT) {
          issues.add(OccurrenceIssue.COORDINATE_REPROJECTION_SUSPICIOUS);
          LOG.debug("Found suspicious shift for datum={} and lat/lon={}/{} so returning failure and keeping orig coord",
            datum, lat, lon);
          return OccurrenceParseResult.fail(new LatLng(lat, lon), issues);
        }
        // flag the record if coords actually changed
        if (lat != lat2 || lon != lon2) {
          issues.add(OccurrenceIssue.COORDINATE_REPROJECTED);
        }
        return OccurrenceParseResult.success(ParseResult.CONFIDENCE.DEFINITE, new LatLng(lat2, lon2), issues);

      }
    } catch (Exception e) {
      issues.add(OccurrenceIssue.COORDINATE_REPROJECTION_FAILED);
      LOG.debug("Coordinate reprojection failed with datum={} and lat/lon={}/{}: {}", datum, lat, lon, e.getMessage());
    }

    return OccurrenceParseResult.fail(new LatLng(lat, lon), issues);
  }

  /**
   * Parses the given datum or SRS code and constructs a full 2D geographic reference system.
   *
   * @return the parsed CRS or null if it can't be interpreted
   */
  @VisibleForTesting
  protected static CoordinateReferenceSystem parseCRS(String datum) {
    CoordinateReferenceSystem crs = null;
    ParseResult<Integer> epsgCode = PARSER.parse(datum);
    if (epsgCode.isSuccessful()) {
      final String code = "EPSG:" + epsgCode.getPayload();

      // first try to create a full fledged CRS from the given code
      try {
        crs = CRS.decode(code);

      } catch (FactoryException e) {
        // that didn't work, maybe it is *just* a datum
        try {
          GeodeticDatum dat = DATUM_FACTORY.createGeodeticDatum(code);
          crs = new DefaultGeographicCRS(dat, DefaultEllipsoidalCS.GEODETIC_2D);

        } catch (FactoryException e1) {
          // also not a datum, no further ideas, log error
          // swallow anything and return null instead
          LOG.info("No CRS or DATUM for given datum code >>{}<<: {}", datum, e1.getMessage());
        }
      }
    }
    return crs;
  }
}
