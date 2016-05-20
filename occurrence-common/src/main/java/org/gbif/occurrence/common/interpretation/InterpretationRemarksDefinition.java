package org.gbif.occurrence.common.interpretation;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * Classification and definition of all the {@link InterpretationRemark}.
 * WORK IN PROGRESS
 */
public class InterpretationRemarksDefinition {

  private static final List<Term> TAXONOMY_TERMS =
          ImmutableList.<Term>builder()
                  .add(DwcTerm.kingdom)
                  .add(DwcTerm.phylum)
                  .add(DwcTerm.class_)
                  .add(DwcTerm.order)
                  .add(DwcTerm.family)
                  .add(DwcTerm.genus)
                  .add(DwcTerm.scientificName)
                  .add(DwcTerm.scientificNameAuthorship)
                  .add(GbifTerm.genericName)
                  .add(DwcTerm.specificEpithet)
                  .add(DwcTerm.infraspecificEpithet)
                  .build();

  public static final List<InterpretationRemark> REMARKS =
          ImmutableList.<InterpretationRemark>builder()
                  // Errors
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.BASIS_OF_RECORD_INVALID, InterpretationRemarkSeverity.ERROR,
                          DwcTerm.basisOfRecord))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.COORDINATE_OUT_OF_RANGE, InterpretationRemarkSeverity.ERROR,
                          DwcTerm.decimalLatitude, DwcTerm.decimalLongitude))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.TAXON_MATCH_NONE, InterpretationRemarkSeverity.ERROR,
                          TAXONOMY_TERMS))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.INTERPRETATION_ERROR, InterpretationRemarkSeverity.ERROR))

                  // Warnings
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.ZERO_COORDINATE, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.decimalLatitude, DwcTerm.decimalLongitude))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.PRESUMED_SWAPPED_COORDINATE, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.decimalLatitude, DwcTerm.decimalLongitude))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.PRESUMED_NEGATED_LONGITUDE, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.decimalLatitude, DwcTerm.decimalLongitude))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.PRESUMED_NEGATED_LATITUDE, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.decimalLatitude, DwcTerm.decimalLongitude))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.COUNTRY_INVALID, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.country))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.COUNTRY_MISMATCH, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.country, DwcTerm.countryCode))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.decimalLatitude, DwcTerm.decimalLongitude, DwcTerm.country))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.country))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.geodeticDatum))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.GEODETIC_DATUM_INVALID, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.geodeticDatum))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.COORDINATE_REPROJECTION_FAILED, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.geodeticDatum, DwcTerm.decimalLatitude, DwcTerm.decimalLongitude))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.COORDINATE_REPROJECTION_SUSPICIOUS, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.geodeticDatum, DwcTerm.decimalLatitude, DwcTerm.decimalLongitude))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.TAXON_MATCH_FUZZY, InterpretationRemarkSeverity.WARNING,
                          TAXONOMY_TERMS))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.TAXON_MATCH_HIGHERRANK, InterpretationRemarkSeverity.WARNING,
                          TAXONOMY_TERMS))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.COORDINATE_PRECISION_INVALID, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.coordinatePrecision))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.COORDINATE_UNCERTAINTY_METERS_INVALID, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.coordinateUncertaintyInMeters))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.RECORDED_DATE_MISMATCH, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.eventDate, DwcTerm.year, DwcTerm.month, DwcTerm.day))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.RECORDED_DATE_INVALID, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.eventDate, DwcTerm.year, DwcTerm.month, DwcTerm.day))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.RECORDED_DATE_UNLIKELY, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.eventDate, DwcTerm.year, DwcTerm.month, DwcTerm.day))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.MODIFIED_DATE_INVALID, InterpretationRemarkSeverity.WARNING,
                          DcTerm.modified))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.MODIFIED_DATE_UNLIKELY, InterpretationRemarkSeverity.WARNING,
                          DcTerm.modified))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.IDENTIFIED_DATE_UNLIKELY, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.dateIdentified))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.IDENTIFIED_DATE_INVALID, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.dateIdentified))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.ELEVATION_NOT_METRIC, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.minimumElevationInMeters, DwcTerm.maximumElevationInMeters))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.ELEVATION_UNLIKELY, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.minimumElevationInMeters, DwcTerm.maximumElevationInMeters))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.ELEVATION_MIN_MAX_SWAPPED, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.minimumElevationInMeters, DwcTerm.maximumElevationInMeters))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.ELEVATION_NON_NUMERIC, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.minimumElevationInMeters, DwcTerm.maximumElevationInMeters))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.DEPTH_NOT_METRIC, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.minimumDepthInMeters, DwcTerm.maximumDepthInMeters))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.DEPTH_UNLIKELY, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.minimumDepthInMeters, DwcTerm.maximumDepthInMeters))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.DEPTH_MIN_MAX_SWAPPED, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.minimumDepthInMeters, DwcTerm.maximumDepthInMeters))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.DEPTH_NON_NUMERIC, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.minimumDepthInMeters, DwcTerm.maximumDepthInMeters))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.TYPE_STATUS_INVALID, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.typeStatus))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.REFERENCES_URI_INVALID, InterpretationRemarkSeverity.WARNING,
                          DcTerm.references))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.INDIVIDUAL_COUNT_INVALID, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.individualCount))
// provided in extension
//                  .add(InterpretationRemark.of(
//                          OccurrenceIssue.MULTIMEDIA_DATE_INVALID, InterpretationRemarkSeverity.WARNING,
//                          DwcTerm.mul))
//                  .add(InterpretationRemark.of(
//                          OccurrenceIssue.MULTIMEDIA_URI_INVALID, InterpretationRemarkSeverity.WARNING,
//                          DwcTerm.individualCount))

                  // Info
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.COORDINATE_ROUNDED, InterpretationRemarkSeverity.INFO,
                          DwcTerm.decimalLatitude, DwcTerm.decimalLongitude))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.COORDINATE_REPROJECTED, InterpretationRemarkSeverity.INFO,
                          DwcTerm.decimalLatitude, DwcTerm.decimalLongitude, DwcTerm.geodeticDatum))
                  .build();

// not used
//  COORDINATE_INVALID,
//  CONTINENT_INVALID,
//  CONTINENT_DERIVED_FROM_COORDINATES,
//  CONTINENT_COUNTRY_MISMATCH,
// ---------

}
