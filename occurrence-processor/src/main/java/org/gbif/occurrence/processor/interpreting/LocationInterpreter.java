package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.GeospatialParseUtils;
import org.gbif.common.parsers.geospatial.IntPrecision;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.processor.interpreting.result.CoordinateCountry;
import org.gbif.occurrence.processor.interpreting.util.CoordinateInterpreter;
import org.gbif.occurrence.processor.interpreting.util.CountryInterpreter;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper for the interpreting steps required to parse and validate location incl coordinates given as latitude and
 * longitude. Intended to be run as its own thread because the underlying web service call takes some time.
 */
public class LocationInterpreter implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(LocationInterpreter.class);

  private final VerbatimOccurrence verbatim;
  private final Occurrence occ;

  public LocationInterpreter(VerbatimOccurrence verbatim, Occurrence occ) {
    this.verbatim = verbatim;
    this.occ = occ;
  }

  @Override
  public void run() {
    Country country = interpretCountry();
    interpretContinent(country);
    interpretCoordinates(country);
    interpretAltitude();
    interpretDepth();
    interpretWaterBody();
    interpretState(country);
  }

  //TODO: improve this method and put it into parsers!
  private String cleanName(String x) {
    x = StringUtils.normalizeSpace(x).trim();
    // if we get all upper names, Capitalize them
    if (StringUtils.isAllUpperCase(StringUtils.deleteWhitespace(x))) {
      x = StringUtils.capitalize(x.toLowerCase());
    }
    return x;
  }

  private void interpretState(Country country) {
    if (verbatim.hasVerbatimField(DwcTerm.stateProvince)) {
      occ.setStateProvince( cleanName(verbatim.getVerbatimField(DwcTerm.stateProvince)) );
    }
    // TODO: verify against country?
  }

  private void interpretContinent(Country country) {
    if (verbatim.hasVerbatimField(DwcTerm.continent)) {
      String cont = cleanName(verbatim.getVerbatimField(DwcTerm.continent));
      try {
        occ.setContinent((Continent) VocabularyUtils.lookupEnum(cont, Continent.class));
      } catch (IllegalArgumentException e) {
        //TODO: set invalid continent issue
      }
    }

    // TODO: if null, try to derive from country
  }

  private void interpretWaterBody() {
    if (verbatim.hasVerbatimField(DwcTerm.waterBody)) {
      occ.setWaterBody(cleanName(verbatim.getVerbatimField(DwcTerm.waterBody)));
    }
  }

  private Country interpretCountry() {
    ParseResult<Country> inter = CountryInterpreter.interpretCountry(verbatim.getVerbatimField(DwcTerm.countryCode), verbatim.getVerbatimField(DwcTerm.country));
    occ.setCountry(inter.getPayload());
    occ.getIssues().addAll(inter.getIssues());
    return occ.getCountry();
  }

  private void interpretCoordinates(Country country) {
    ParseResult<CoordinateCountry> coordLookup = CoordinateInterpreter.interpretCoordinates(
      verbatim.getVerbatimField(DwcTerm.decimalLatitude), verbatim.getVerbatimField(DwcTerm.decimalLongitude), country);

    if (coordLookup.getPayload().getLatitude() == null
        && verbatim.hasVerbatimField(DwcTerm.verbatimLatitude) && verbatim.hasVerbatimField(DwcTerm.verbatimLongitude)) {
      LOG.debug("Try verbatim coordinates");
      // try again with verbatim lat/lon
      coordLookup = CoordinateInterpreter.interpretCoordinates(
        verbatim.getVerbatimField(DwcTerm.verbatimLatitude), verbatim.getVerbatimField(DwcTerm.verbatimLongitude), country);
    }

    occ.setDecimalLatitude(coordLookup.getPayload().getLatitude());
    occ.setDecimalLongitude(coordLookup.getPayload().getLongitude());
    occ.getIssues().addAll(coordLookup.getIssues());

    //TODO: interpret also coordinateUncertaintyInMeters
    if (verbatim.hasVerbatimField(DwcTerm.coordinatePrecision)) {
      // accept negative precisions and mirror
      double prec = Math.abs(NumberUtils.toDouble(verbatim.getVerbatimField(DwcTerm.coordinatePrecision).trim()));
      if (prec != 0) {
        // accuracy equals the precision in the case of decimal lat / lon
        if (prec > 10) {
          // add issue for unlikely coordinatePrecision
        } else {
          occ.setCoordinateAccuracy(prec);
        }
      }
    }

    LOG.debug("Got lat [{}] lng [{}]", coordLookup.getPayload().getLatitude(), coordLookup.getPayload().getLongitude());
  }

  private void interpretDepth() {
    ParseResult<IntPrecision> result = GeospatialParseUtils.parseDepth(verbatim.getVerbatimField(DwcTerm.minimumDepthInMeters),
                                                                       verbatim.getVerbatimField(DwcTerm.maximumDepthInMeters),
                                                                       null);
    if (result.isSuccessful() && result.getPayload().getValue() != null) {
      occ.setDepth( result.getPayload().getValue() );
      occ.setDepthAccuracy(result.getPayload().getPrecision());
      occ.getIssues().addAll(result.getIssues());
    }
  }

  private void interpretDistanceAboveSurface() {
    ParseResult<IntPrecision> result = GeospatialParseUtils.parseAltitude(verbatim.getVerbatimField(DwcTerm.minimumDistanceAboveSurfaceInMeters),
                                                                          verbatim.getVerbatimField(DwcTerm.maximumDistanceAboveSurfaceInMeters),
                                                                          null);
    if (result.isSuccessful() && result.getPayload().getValue() != null) {
      occ.setDistanceAboveSurface(result.getPayload().getValue());
      occ.setDistanceAboveSurfaceAccuracy(result.getPayload().getPrecision());
      //TODO: http://dev.gbif.org/issues/browse/POR-1817
      //occ.getIssues().addAll(result.getIssues());
    }
  }

  private void interpretAltitude() {
    ParseResult<IntPrecision> result = GeospatialParseUtils.parseAltitude(verbatim.getVerbatimField(DwcTerm.minimumElevationInMeters),
                                                                                verbatim.getVerbatimField(DwcTerm.maximumElevationInMeters),
                                                                                null);
    if (result.isSuccessful() && result.getPayload().getValue() != null) {
      occ.setElevation( result.getPayload().getValue() );
      occ.setElevationAccuracy(result.getPayload().getPrecision());
      occ.getIssues().addAll(result.getIssues());
    }
  }

}
