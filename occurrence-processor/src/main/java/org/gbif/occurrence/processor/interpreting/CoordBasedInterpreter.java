package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.Country;
import org.gbif.occurrence.interpreters.CoordinateInterpreter;
import org.gbif.occurrence.interpreters.CountryInterpreter;
import org.gbif.occurrence.interpreters.result.CoordinateInterpretationResult;
import org.gbif.occurrence.persistence.api.VerbatimOccurrence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper for the interpreting steps required to parse and validate coordinates given as latitude and longitude.
 * Intended to be run as its own thread because the underlying web service call takes some time.
 */
public class CoordBasedInterpreter implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(CoordBasedInterpreter.class);

  private final VerbatimOccurrence verbatim;
  private final Occurrence occ;

  public CoordBasedInterpreter(VerbatimOccurrence verbatim, Occurrence occ) {
    this.verbatim = verbatim;
    this.occ = occ;
  }

  @Override
  public void run() {
    String cleanedCountry = CountryInterpreter.interpretCountry(verbatim.getCountry());
    CoordinateInterpretationResult coordLookup =
      CoordinateInterpreter.interpretCoordinates(verbatim.getLatitude(), verbatim.getLongitude(), cleanedCountry);

    interpretCoords(occ, coordLookup);

    interpretOtherIssue(occ, cleanedCountry, coordLookup);

    interpretGeospatialIssue(occ, coordLookup);

    interpretCountry(occ, cleanedCountry, coordLookup);
  }

  private static void interpretCountry(Occurrence occ, String cleanedCountry,
    CoordinateInterpretationResult coordLookup) {
    String isoCountryCode =
      coordLookup.getCountryCode() != null && coordLookup.getCountryCode().length() == 2 ? coordLookup
        .getCountryCode() : cleanedCountry;
    if (isoCountryCode != null) {
      occ.setCountry(Country.fromIsoCode(isoCountryCode));
    }
    LOG.debug("Got iso country [{}]", occ.getCountry());
  }

  private static void interpretGeospatialIssue(Occurrence occ, CoordinateInterpretationResult coordLookup) {
    // TODO: adapt to new enum based issues
//    occ.setGeospatialIssue(coordLookup.getGeoSpatialIssue());
    LOG.debug("Got geospatial issue [{}]", coordLookup.getGeoSpatialIssue());
  }

  private static void interpretOtherIssue(Occurrence occ, String cleanedCountry,
    CoordinateInterpretationResult coordLookup) {
    // TODO: adapt to new enum based issues
//    if (cleanedCountry == null && coordLookup.getCountryCode() != null) {
//      occ.setOtherIssue(8);
//    } else {
//      occ.setOtherIssue(0);
//    }
//    LOG.debug("Got other issue [{}]", occ.getOtherIssue());
  }

  private static void interpretCoords(Occurrence occ, CoordinateInterpretationResult coordLookup) {
    occ.setLatitude(coordLookup.getLatitude());
    occ.setLongitude(coordLookup.getLongitude());
    LOG.debug("Got lat [{}] lng [{}]", coordLookup.getLatitude(), coordLookup.getLongitude());
  }
}
