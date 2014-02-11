package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.GeospatialParseUtils;
import org.gbif.common.parsers.geospatial.IntPrecision;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.processor.interpreting.result.CoordinateCountry;
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
    removeVerbatimTerms();
  }

  private void removeVerbatimTerms() {
    Term[] terms = new Term[]{
      DwcTerm.decimalLatitude, DwcTerm.decimalLongitude, DwcTerm.verbatimCoordinates, DwcTerm.geodeticDatum,
      DwcTerm.verbatimLatitude, DwcTerm.verbatimLongitude,
      DwcTerm.countryCode, DwcTerm.country,
      DwcTerm.minimumDepthInMeters, DwcTerm.maximumDepthInMeters,
      DwcTerm.minimumElevationInMeters, DwcTerm.maximumElevationInMeters,
    };
    for (Term t : terms) {
      occ.getVerbatimFields().remove(t);
    }
  }

  private Country interpretCountry() {
    ParseResult<Country> inter = CountryInterpreter.interpretCountry(verbatim.getVerbatimField(DwcTerm.countryCode), verbatim.getVerbatimField(DwcTerm.country));
    occ.setCountry(inter.getPayload());
    occ.getIssues().addAll(inter.getIssues());
    return occ.getCountry();
  }

  private void doCoordinateLookup(Country country) {
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

    LOG.debug("Got lat [{}] lng [{}]", coordLookup.getPayload().getLatitude(), coordLookup.getPayload().getLongitude());
  }

  private void interpretDepth() {
    ParseResult<IntPrecision> result = GeospatialParseUtils.parseAltitude(verbatim.getVerbatimField(DwcTerm.minimumDepthInMeters),
                                                                                verbatim.getVerbatimField(DwcTerm.maximumDepthInMeters),
                                                                                null);
    if (result.isSuccessful() && result.getPayload().getValue() != null) {
      occ.setDepth( result.getPayload().getValue() );
      occ.setDepthAccuracy(result.getPayload().getPrecision());
      occ.getIssues().addAll(result.getIssues());
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
