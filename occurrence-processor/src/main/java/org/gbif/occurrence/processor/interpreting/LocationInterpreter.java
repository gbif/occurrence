package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.ContinentParser;
import org.gbif.common.parsers.CountryParser;
import org.gbif.common.parsers.NumberParser;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.DoubleAccuracy;
import org.gbif.common.parsers.geospatial.MeterRangeParser;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.processor.interpreting.result.CoordinateResult;
import org.gbif.occurrence.processor.interpreting.util.CountryMaps;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper for the interpreting steps required to parse and validate location incl coordinates given as latitude and
 * longitude.
 */
public class LocationInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(LocationInterpreter.class);

  private static final CountryParser PARSER = CountryParser.getInstance();

  private final CoordinateInterpreter coordinateInterpreter;
  // taken from https://en.wikipedia.org/wiki/Geographic_coordinate_system#Expressing_latitude_and_longitude_as_linear_units
  private static final int LAT_DEGREE_IN_METER = 110580;

  @Inject
  public LocationInterpreter(CoordinateInterpreter coordinateInterpreter) {
    this.coordinateInterpreter = coordinateInterpreter;
  }

  public void interpretLocation(VerbatimOccurrence verbatim, Occurrence occ) {
    Country country = interpretCountry(verbatim, occ);
    interpretCoordinates(verbatim, occ, country);

    interpretContinent(verbatim, occ);
    interpretWaterBody(verbatim, occ);
    interpretState(verbatim, occ);

    interpretElevation(verbatim, occ);
    interpretDepth(verbatim, occ);
  }

  //TODO: improve this method and put it into parsers!
  private static String cleanName(String x) {
    x = StringUtils.normalizeSpace(x).trim();
    // if we get all upper names, Capitalize them
    if (StringUtils.isAllUpperCase(StringUtils.deleteWhitespace(x))) {
      x = StringUtils.capitalize(x.toLowerCase());
    }
    return x;
  }

  /**
   * Attempts to convert given country strings to a single country, verifying the all interpreted countries
   * do not contradict.
   *
   * @param country verbatim country strings, e.g. dwc:country or dwc:countryCode
   */
  public OccurrenceParseResult<Country> interpretCountry(String ... country) {
    if (country == null) {
      return OccurrenceParseResult.fail();
    }

    OccurrenceParseResult<Country> result = null;
    for (String verbatim : country) {
      if (!Strings.isNullOrEmpty(verbatim)) {
        if (result == null) {
          result = new OccurrenceParseResult(PARSER.parse(verbatim));

        } else if (result.isSuccessful()) {
          ParseResult<Country> result2 = PARSER.parse(verbatim);
          if (result2.isSuccessful()) {
            // only inspect secondary parsing if its also successful
            if (!result2.getPayload().equals(result.getPayload())) {
              result.getIssues().add(OccurrenceIssue.COUNTRY_MISMATCH);
            }
          }

        } else {
          // failed before. Use new parsing and add issue
          result = new OccurrenceParseResult(PARSER.parse(verbatim));
          result.getIssues().add(OccurrenceIssue.COUNTRY_INVALID);
        }
      }
    }

    if (result == null) {
      // we got an array of null or empty countries passed in
      return OccurrenceParseResult.fail();
    }

    if (!result.isSuccessful()) {
      result.getIssues().add(OccurrenceIssue.COUNTRY_INVALID);
    }
    return result;
  }

  private void interpretState(VerbatimOccurrence verbatim, Occurrence occ) {
    if (verbatim.hasVerbatimField(DwcTerm.stateProvince)) {
      occ.setStateProvince(cleanName(verbatim.getVerbatimField(DwcTerm.stateProvince)));
    }
    // TODO: verify against country?
  }

  private void interpretContinent(VerbatimOccurrence verbatim, Occurrence occ) {
    if (verbatim.hasVerbatimField(DwcTerm.continent)) {
      ParseResult<Continent> inter = ContinentParser.getInstance().parse(verbatim.getVerbatimField(DwcTerm.continent));
      occ.setContinent(inter.getPayload());
    }

    // TODO: if null, try to derive from country
  }

  private void interpretWaterBody(VerbatimOccurrence verbatim, Occurrence occ) {
    if (verbatim.hasVerbatimField(DwcTerm.waterBody)) {
      occ.setWaterBody(cleanName(verbatim.getVerbatimField(DwcTerm.waterBody)));
    }
  }

  private Country interpretCountry(VerbatimOccurrence verbatim, Occurrence occ) {
    OccurrenceParseResult<Country>
      inter = interpretCountry(verbatim.getVerbatimField(DwcTerm.countryCode),
      verbatim.getVerbatimField(DwcTerm.country));
    occ.setCountry(CountryMaps.preferred(inter.getPayload()));
    occ.getIssues().addAll(inter.getIssues());
    return occ.getCountry();
  }

  private void interpretCoordinates(VerbatimOccurrence verbatim, Occurrence occ, Country country) {
    OccurrenceParseResult<CoordinateResult> parsedCoord = coordinateInterpreter.interpretCoordinate(
            verbatim.getVerbatimField(DwcTerm.decimalLatitude), verbatim.getVerbatimField(DwcTerm.decimalLongitude),
            verbatim.getVerbatimField(DwcTerm.geodeticDatum), country);

    if (!parsedCoord.isSuccessful() && verbatim.hasVerbatimField(DwcTerm.verbatimLatitude)
        && verbatim.hasVerbatimField(DwcTerm.verbatimLongitude)) {
      LOG.debug("Decimal coord interpretation, trying verbatim lat/lon");
      // try again with verbatim lat/lon
      parsedCoord = coordinateInterpreter.interpretCoordinate(verbatim.getVerbatimField(DwcTerm.verbatimLatitude),
                                                              verbatim.getVerbatimField(DwcTerm.verbatimLongitude),
                                                              verbatim.getVerbatimField(DwcTerm.geodeticDatum),country);
    }

    if (!parsedCoord.isSuccessful() && verbatim.hasVerbatimField(DwcTerm.verbatimCoordinates)) {
      LOG.debug("Verbatim lat/lon interpretation, trying single verbatimCoordinates");
      // try again with verbatim coordinates
      parsedCoord = coordinateInterpreter.interpretCoordinate(verbatim.getVerbatimField(DwcTerm.verbatimCoordinates),
                                                              verbatim.getVerbatimField(DwcTerm.geodeticDatum),country);
    }

    if (parsedCoord.isSuccessful() && parsedCoord.getPayload() != null) {
      occ.setDecimalLatitude(parsedCoord.getPayload().getLatitude());
      occ.setDecimalLongitude(parsedCoord.getPayload().getLongitude());

      // If the country returned by the co-ordinate interpreter is different, then it's an acceptable
      // swap (e.g. Réunion→France).
      if (country == null || (country != parsedCoord.getPayload().getCountry())) {
        occ.setCountry(parsedCoord.getPayload().getCountry());
      }

      // interpret coordinateUncertaintyInMeters and coordinatePrecision
      interpretCoordinateUncertaintyAndPrecision(occ, verbatim);
    }

    LOG.debug("Adding coord issues to occ [{}]", parsedCoord.getIssues());
    occ.getIssues().addAll(parsedCoord.getIssues());
  }

  /**
   * http://dev.gbif.org/issues/browse/POR-1804
   * @param occ
   * @param verbatim
   */
  @VisibleForTesting
  protected void interpretCoordinateUncertaintyAndPrecision(Occurrence occ, VerbatimOccurrence verbatim) {
    if (verbatim.hasVerbatimField(DwcTerm.coordinatePrecision)) {
      Double coordinatePrecision = NumberParser.parseDouble(verbatim.getVerbatimField(DwcTerm.coordinatePrecision).trim());
      if(coordinatePrecision != null){
        coordinatePrecision = Math.abs(coordinatePrecision);
      }

      // accepted values are 0, greater than or equal to 1. Since "1.0" means nearest degree, not sure what a value
      // greater than 1 means.
      if (coordinatePrecision != null && coordinatePrecision.doubleValue() >= 0 &&
              coordinatePrecision.doubleValue() <= 1) {
        occ.setCoordinatePrecision(coordinatePrecision);
      }
      else{
        occ.getIssues().add(OccurrenceIssue.COORDINATE_PRECISION_INVALID);
        LOG.debug("Ignoring coordinatePrecision, value invalid or highly unlikely");
      }
    }

    if (verbatim.hasVerbatimField(DwcTerm.coordinateUncertaintyInMeters)) {
      ParseResult<Double> meters = MeterRangeParser.parseMeters(verbatim.getVerbatimField(DwcTerm.coordinateUncertaintyInMeters).trim());
      Double coordinateUncertaintyInMeters = meters.isSuccessful() ? Math.abs(meters.getPayload()) : null;
      if (coordinateUncertaintyInMeters != null && coordinateUncertaintyInMeters != 0 &&
              coordinateUncertaintyInMeters > 10) {
        occ.setCoordinateUncertaintyInMeters(coordinateUncertaintyInMeters);
      } else {
        occ.getIssues().add(OccurrenceIssue.COORDINATE_UNCERTAINTY_METERS_INVALID);
      }
    }
  }

  private void interpretDepth(VerbatimOccurrence verbatim, Occurrence occ) {
    OccurrenceParseResult<DoubleAccuracy> result = MeterRangeParser
      .parseDepth(verbatim.getVerbatimField(DwcTerm.minimumDepthInMeters),
        verbatim.getVerbatimField(DwcTerm.maximumDepthInMeters), null);
    if (result.isSuccessful() && result.getPayload().getValue() != null) {
      occ.setDepth(result.getPayload().getValue());
      occ.setDepthAccuracy(result.getPayload().getAccuracy());
      occ.getIssues().addAll(result.getIssues());
    }
  }

  private void interpretElevation(VerbatimOccurrence verbatim, Occurrence occ) {
    OccurrenceParseResult<DoubleAccuracy> result = MeterRangeParser
      .parseElevation(verbatim.getVerbatimField(DwcTerm.minimumElevationInMeters),
        verbatim.getVerbatimField(DwcTerm.maximumElevationInMeters), null);
    if (result.isSuccessful() && result.getPayload().getValue() != null) {
      occ.setElevation(result.getPayload().getValue());
      occ.setElevationAccuracy(result.getPayload().getAccuracy());
      occ.getIssues().addAll(result.getIssues());
    }

    //TODO: use continent information to get finer unlikely values:
    // http://en.wikipedia.org/wiki/Extremes_on_Earth#Extreme_elevations_and_temperatures_per_continent
  }

}
