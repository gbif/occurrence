package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.ParseResult;
import org.gbif.common.parsers.geospatial.GeospatialParseUtils;
import org.gbif.common.parsers.geospatial.IntPrecisionIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.processor.interpreting.result.CoordinateInterpretationResult;
import org.gbif.occurrence.processor.interpreting.result.InterpretationResult;
import org.gbif.occurrence.processor.interpreting.util.CoordinateInterpreter;
import org.gbif.occurrence.processor.interpreting.util.CountryInterpreter;

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
    doCoordinateLookup(country);
    interpretAltitude();
    interpretDepth();
  }

  private Country interpretCountry() {
    InterpretationResult<Country> inter = CountryInterpreter.interpretCountry(verbatim.getField(DwcTerm.countryCode), verbatim.getField(DwcTerm.country));
    occ.setCountry(inter.getPayload());
    occ.getIssues().addAll(inter.getIssues());
    return occ.getCountry();
  }

  private void doCoordinateLookup(Country country) {
    CoordinateInterpretationResult coordLookup = CoordinateInterpreter.interpretCoordinates(
      verbatim.getField(DwcTerm.decimalLatitude), verbatim.getField(DwcTerm.decimalLongitude), country);

    occ.setLatitude(coordLookup.getLatitude());
    occ.setLongitude(coordLookup.getLongitude());
    occ.getIssues().addAll(coordLookup.getIssues());

    LOG.debug("Got lat [{}] lng [{}]", coordLookup.getLatitude(), coordLookup.getLongitude());
  }

  private void interpretDepth() {
    ParseResult<IntPrecisionIssue> result = GeospatialParseUtils.parseAltitude(verbatim.getField(DwcTerm.minimumDepthInMeters),
                                                                                verbatim.getField(DwcTerm.maximumDepthInMeters),
                                                                                null);
    if (result.isSuccessful() && result.getPayload().getValue() != null) {
      occ.setDepth( result.getPayload().getValue() );
      occ.setDepthAccuracy(result.getPayload().getPrecision());
      occ.getIssues().addAll(result.getPayload().getIssues());
    }
  }

  private void interpretAltitude() {
    ParseResult<IntPrecisionIssue> result = GeospatialParseUtils.parseAltitude(verbatim.getField(DwcTerm.minimumElevationInMeters),
                                                                                verbatim.getField(DwcTerm.maximumElevationInMeters),
                                                                                null);
    if (result.isSuccessful() && result.getPayload().getValue() != null) {
      occ.setAltitude( result.getPayload().getValue() );
      occ.setAltitudeAccuracy(result.getPayload().getPrecision());
      occ.getIssues().addAll(result.getPayload().getIssues());
    }
  }

}
