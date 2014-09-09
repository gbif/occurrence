package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.ContinentParser;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.DoubleAccuracy;
import org.gbif.common.parsers.geospatial.MeterRangeParser;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.processor.interpreting.result.CoordinateResult;
import org.gbif.occurrence.processor.interpreting.util.CoordinateInterpreter;
import org.gbif.occurrence.processor.interpreting.util.CountryInterpreter;

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

  public static void interpretLocation(VerbatimOccurrence verbatim, Occurrence occ) {
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

  private static void interpretState(VerbatimOccurrence verbatim, Occurrence occ, Country country) {
    if (verbatim.hasVerbatimField(DwcTerm.stateProvince)) {
      occ.setStateProvince(cleanName(verbatim.getVerbatimField(DwcTerm.stateProvince)));
    }
    // TODO: verify against country?
  }

  private static void interpretContinent(VerbatimOccurrence verbatim, Occurrence occ, Country country) {
    if (verbatim.hasVerbatimField(DwcTerm.continent)) {
      ParseResult<Continent> inter = ContinentParser.getInstance().parse(verbatim.getVerbatimField(DwcTerm.continent));
      occ.setContinent(inter.getPayload());
    }

    // TODO: if null, try to derive from country
  }

  private static void interpretWaterBody(VerbatimOccurrence verbatim, Occurrence occ) {
    if (verbatim.hasVerbatimField(DwcTerm.waterBody)) {
      occ.setWaterBody(cleanName(verbatim.getVerbatimField(DwcTerm.waterBody)));
    }
  }

  private static Country interpretCountry(VerbatimOccurrence verbatim, Occurrence occ) {
    OccurrenceParseResult<Country>
      inter = CountryInterpreter.interpretCountry(verbatim.getVerbatimField(DwcTerm.countryCode),
      verbatim.getVerbatimField(DwcTerm.country));
    occ.setCountry(inter.getPayload());
    occ.getIssues().addAll(inter.getIssues());
    return occ.getCountry();
  }

  private static void interpretCoordinates(VerbatimOccurrence verbatim, Occurrence occ, Country country) {
    OccurrenceParseResult<CoordinateResult> parsedCoord = CoordinateInterpreter.interpretCoordinate(
          verbatim.getVerbatimField(DwcTerm.decimalLatitude), verbatim.getVerbatimField(DwcTerm.decimalLongitude),
          verbatim.getVerbatimField(DwcTerm.geodeticDatum), country);

    if (!parsedCoord.isSuccessful() && verbatim.hasVerbatimField(DwcTerm.verbatimLatitude)
        && verbatim.hasVerbatimField(DwcTerm.verbatimLongitude)) {
      LOG.debug("Decimal coord interpretation, trying verbatim lat/lon");
      // try again with verbatim lat/lon
      parsedCoord = CoordinateInterpreter.interpretCoordinate(verbatim.getVerbatimField(DwcTerm.verbatimLatitude),
                                                              verbatim.getVerbatimField(DwcTerm.verbatimLongitude),
                                                              verbatim.getVerbatimField(DwcTerm.geodeticDatum),country);
    }

    if (!parsedCoord.isSuccessful() && verbatim.hasVerbatimField(DwcTerm.verbatimCoordinates)) {
      LOG.debug("Verbatim lat/lon interpretation, trying single verbatimCoordinates");
      // try again with verbatim coordinates
      parsedCoord = CoordinateInterpreter.interpretCoordinate(verbatim.getVerbatimField(DwcTerm.verbatimCoordinates),
                                                              verbatim.getVerbatimField(DwcTerm.geodeticDatum),country);
    }

    if (parsedCoord.isSuccessful() && parsedCoord.getPayload() != null) {
      occ.setDecimalLatitude(parsedCoord.getPayload().getLatitude());
      occ.setDecimalLongitude(parsedCoord.getPayload().getLongitude());
      if (country == null) {
        occ.setCountry(parsedCoord.getPayload().getCountry());
      }
      //TODO: interpret also coordinateUncertaintyInMeters
      if (verbatim.hasVerbatimField(DwcTerm.coordinatePrecision)) {
        // accept negative precisions and mirror
        double prec = Math.abs(NumberUtils.toDouble(verbatim.getVerbatimField(DwcTerm.coordinatePrecision).trim()));
        if (prec != 0) {
          // accuracy equals the precision in the case of decimal lat / lon
          if (prec > 10) {
            // add issue for unlikely coordinatePrecision
            // TODO: this happens alot - maybe not so unlikely?
            LOG.debug("Ignoring coordinatePrecision > 10 as highly unlikely");
          } else {
            occ.setCoordinateAccuracy(prec);
          }
        }
      }
      LOG.debug("Got lat [{}] lng [{}]", parsedCoord.getPayload().getLatitude(),
        parsedCoord.getPayload().getLongitude());
    }

    LOG.debug("Adding coord issues to occ [{}]", parsedCoord.getIssues());
    occ.getIssues().addAll(parsedCoord.getIssues());
  }

  private static void interpretDepth(VerbatimOccurrence verbatim, Occurrence occ) {
    OccurrenceParseResult<DoubleAccuracy> result = MeterRangeParser
      .parseDepth(verbatim.getVerbatimField(DwcTerm.minimumDepthInMeters),
        verbatim.getVerbatimField(DwcTerm.maximumDepthInMeters), null);
    if (result.isSuccessful() && result.getPayload().getValue() != null) {
      occ.setDepth(result.getPayload().getValue());
      occ.setDepthAccuracy(result.getPayload().getAccuracy());
      occ.getIssues().addAll(result.getIssues());
    }
  }

  private static void interpretElevation(VerbatimOccurrence verbatim, Occurrence occ) {
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
