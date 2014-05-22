package org.gbif.occurrence.processor.interpreting.util;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.DatumParser;
import org.gbif.common.parsers.geospatial.LatLng;

import java.util.EnumSet;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
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

/**
 * Utils class that reprojects to WGS84 based on geotools transformations and SRS databases.
 * It allows to add custom reference systems via property files,
 * see also:
 * - http://docs.geotools.org/latest/userguide/library/referencing/faq.html#q-how-to-i-add-my-own-epsg-codes
 * - http://docs.geotools.org/latest/userguide/library/referencing/wkt.html
 */
public class Wgs84Projection {

  private static final Logger LOG = LoggerFactory.getLogger(Wgs84Projection.class);
  private static final DatumParser PARSER = DatumParser.getInstance();
  private static final DatumAuthorityFactory DATUM_FACTORY = BasicFactories.getDefault().getDatumAuthorityFactory();
  private static final double SUSPICIOUS_SHIFT = 0.1d;

  /**
   * Reproject the given coordinates into WGS84 coordinates based on a known source datum or SRS.
   * Darwin Core allows not only geodetic datums but also full spatial reference systems as values for "datum".
   * <p/>
   * The method will always return lat lons even if the processing failed. In that case only issues are set and the
   * parsing result set to fail - but with a valid payload.
   *
   * @param lat   the original latitude
   * @param lon   the original longitude
   * @param datum the original geodetic datum the coordinates are in
   *
   * @return the reprojected coordinates or the original ones in case transformation failed
   */
  public static ParseResult<LatLng> reproject(double lat, double lon, String datum) {
    Preconditions.checkArgument(lat >= -90d && lat <= 90d);
    Preconditions.checkArgument(lon >= -180d && lon <= 180d);

    if (Strings.isNullOrEmpty(datum)) {
      return ParseResult.success(ParseResult.CONFIDENCE.DEFINITE, new LatLng(lat, lon));
    }

    Set<OccurrenceIssue> issues = EnumSet.noneOf(OccurrenceIssue.class);

    try {
      CoordinateReferenceSystem crs = parseCRS(datum);
      if (crs == null) {
        issues.add(OccurrenceIssue.GEODETIC_DATUM_INVALID);

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

        // verify the datum shift is reasonable
        if (Math.abs(lat-dstPt[1]) > SUSPICIOUS_SHIFT || Math.abs(lon-dstPt[0]) > SUSPICIOUS_SHIFT) {
          issues.add(OccurrenceIssue.COORDINATE_REPROJECTION_SUSPICIOUS);
        }
        return ParseResult.success(ParseResult.CONFIDENCE.DEFINITE, new LatLng(dstPt[1], dstPt[0]), issues);

      }
    } catch (Exception e) {
      issues.add(OccurrenceIssue.COORDINATE_REPROJECTION_FAILED);
      LOG.debug("Coordinate reprojection failed with datum={} and lat/lon={}/{}: {}", datum, lat, lon, e.getMessage());
    }

    return ParseResult.fail(new LatLng(lat, lon), issues);
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
