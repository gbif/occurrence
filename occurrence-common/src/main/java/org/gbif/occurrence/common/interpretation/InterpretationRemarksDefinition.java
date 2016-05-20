package org.gbif.occurrence.common.interpretation;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * Classification and definition of all the {@link InterpretationRemark}.
 * WORK IN PROGRESS
 */
public class InterpretationRemarksDefinition {

  public static List<InterpretationRemark> REMARKS =
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
                          DwcTerm.scientificName)) //probably all others terms too (class, order, ...)

                  // Warnings
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.ZERO_COORDINATE, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.decimalLatitude, DwcTerm.decimalLongitude))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.COUNTRY_INVALID, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.country))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.decimalLatitude, DwcTerm.decimalLongitude, DwcTerm.country))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.country))
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.geodeticDatum))
                  // Info
                  .add(InterpretationRemark.of(
                          OccurrenceIssue.COORDINATE_ROUNDED, InterpretationRemarkSeverity.INFO,
                          DwcTerm.decimalLatitude, DwcTerm.decimalLongitude))
                  .build();

//  x ZERO_COORDINATE,
//  x COUNTRY_INVALID,
//  x GEODETIC_DATUM_ASSUMED_WGS84,
//  x BASIS_OF_RECORD_INVALID,
//  x COUNTRY_COORDINATE_MISMATCH,
//  x COORDINATE_OUT_OF_RANGE,
//  x COORDINATE_ROUNDED,
//  x TAXON_MATCH_NONE
//  x COUNTRY_DERIVED_FROM_COORDINATES

//  COORDINATE_INVALID,
//  GEODETIC_DATUM_INVALID,
//  COORDINATE_REPROJECTED,
//  COORDINATE_REPROJECTION_FAILED,
//  COORDINATE_REPROJECTION_SUSPICIOUS,
//  COORDINATE_PRECISION_INVALID,
//  COORDINATE_UNCERTAINTY_METERS_INVALID,
//  COUNTRY_MISMATCH,
//  ,
//  CONTINENT_COUNTRY_MISMATCH,
//  CONTINENT_INVALID,
//  CONTINENT_DERIVED_FROM_COORDINATES,
//  PRESUMED_SWAPPED_COORDINATE,
//  PRESUMED_NEGATED_LONGITUDE,
//  PRESUMED_NEGATED_LATITUDE,
//  RECORDED_DATE_MISMATCH,
//  RECORDED_DATE_INVALID,
//  RECORDED_DATE_UNLIKELY,
//  TAXON_MATCH_FUZZY,
//  TAXON_MATCH_HIGHERRANK,
//  ,
//  DEPTH_NOT_METRIC,
//  DEPTH_UNLIKELY,
//  DEPTH_MIN_MAX_SWAPPED,
//  DEPTH_NON_NUMERIC,
//  ELEVATION_UNLIKELY,
//  ELEVATION_MIN_MAX_SWAPPED,
//  ELEVATION_NOT_METRIC,
//  ELEVATION_NON_NUMERIC,
//  MODIFIED_DATE_INVALID,
//  MODIFIED_DATE_UNLIKELY,
//  IDENTIFIED_DATE_UNLIKELY,
//  IDENTIFIED_DATE_INVALID,

//  TYPE_STATUS_INVALID,
//  MULTIMEDIA_DATE_INVALID,
//  MULTIMEDIA_URI_INVALID,
//  REFERENCES_URI_INVALID,
//  INTERPRETATION_ERROR,
//  INDIVIDUAL_COUNT_INVALID;

}
