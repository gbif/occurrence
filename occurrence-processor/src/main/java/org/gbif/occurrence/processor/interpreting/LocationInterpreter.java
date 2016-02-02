package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.util.LengthUtils;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.ContinentParser;
import org.gbif.common.parsers.CountryParser;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.DoubleAccuracy;
import org.gbif.common.parsers.geospatial.MeterRangeParser;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.processor.interpreting.result.CoordinateResult;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
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

    interpretContinent(verbatim, occ, country);
    interpretWaterBody(verbatim, occ);
    interpretState(verbatim, occ, country);

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

  private void interpretState(VerbatimOccurrence verbatim, Occurrence occ, Country country) {
    if (verbatim.hasVerbatimField(DwcTerm.stateProvince)) {
      occ.setStateProvince(cleanName(verbatim.getVerbatimField(DwcTerm.stateProvince)));
    }
    // TODO: verify against country?
  }

  private void interpretContinent(VerbatimOccurrence verbatim, Occurrence occ, Country country) {
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
    occ.setCountry(inter.getPayload());
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

      // C.G., 2015/11/17 urgent patch, allow country overwrites if record is indicated as coming from IRELAND but
      // coordinateInterpreter says UNITED_KINGDOM
      if (country == null || (country == Country.IRELAND && parsedCoord.getPayload().getCountry() == Country.UNITED_KINGDOM )) {
        occ.setCountry(parsedCoord.getPayload().getCountry());
      }

      // interpret coordinateAccurracy
      interpretCoordAccurracy(occ, verbatim);

      LOG.debug("Got lat [{}] lng [{}] accurracy={}", occ.getDecimalLatitude(), occ.getDecimalLongitude(), occ.getCoordinateAccuracy());
    }

    LOG.debug("Adding coord issues to occ [{}]", parsedCoord.getIssues());
    occ.getIssues().addAll(parsedCoord.getIssues());
  }

  /**
   * http://dev.gbif.org/issues/browse/POR-1804
   * @param occ
   * @param verbatim
   */
  private void interpretCoordAccurracy(Occurrence occ, VerbatimOccurrence verbatim) {
    if (verbatim.hasVerbatimField(DwcTerm.coordinatePrecision)) {
      // accept negative precisions and mirror
      double prec = Math.abs(NumberUtils.toDouble(verbatim.getVerbatimField(DwcTerm.coordinatePrecision).trim()));
      if (prec != 0) {
        // accuracy equals the precision in the case of decimal lat / lon
        // TODO: this happens alot - maybe not so unlikely or value is in meters instead?
        if (prec > 10) {
          // add issue for unlikely coordinatePrecision
          occ.getIssues().add(OccurrenceIssue.COORDINATE_ACCURACY_INVALID);
          LOG.debug("Ignoring coordinatePrecision > 10 as highly unlikely");
        } else {
          occ.setCoordinateAccuracy(prec);
        }
      }
    }

    if (verbatim.hasVerbatimField(DwcTerm.coordinateUncertaintyInMeters)) {
      // accept negative precisions and mirror
      ParseResult<Double> meters = MeterRangeParser.parseMeters(verbatim.getVerbatimField(DwcTerm.coordinateUncertaintyInMeters).trim());
      if (meters.isSuccessful()) {
        double accurracy = LengthUtils.metersToLatDegree(meters.getPayload());
        if (accurracy > 0) {
          // do we have an accurracy already and do they match up?
          if (accurracy > 10) {
            occ.getIssues().add(OccurrenceIssue.COORDINATE_ACCURACY_INVALID);
            LOG.debug("Ignoring coordinatePrecision > 10 as highly unlikely");

          } else if (occ.getCoordinateAccuracy() != null) {
            if (Math.abs(occ.getCoordinateAccuracy() - accurracy) > 0.01) {
              occ.getIssues().add(OccurrenceIssue.COORDINATE_PRECISION_UNCERTAINTY_MISMATCH);
              LOG.debug("coordinateAccurracy derived from precision {} is different from uncertaintyInMeters {}", occ.getCoordinateAccuracy(), accurracy);
            }

          } else {
            occ.setCoordinateAccuracy(accurracy);
          }

        } else {
          occ.getIssues().add(OccurrenceIssue.COORDINATE_ACCURACY_INVALID);
        }
      } else {
        occ.getIssues().add(OccurrenceIssue.COORDINATE_ACCURACY_INVALID);
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
