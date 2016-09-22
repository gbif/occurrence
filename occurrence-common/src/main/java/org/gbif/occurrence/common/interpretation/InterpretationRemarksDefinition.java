package org.gbif.occurrence.common.interpretation;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import java.util.Set;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;

/**
 * Definition and classification of all the {@link InterpretationRemark}.
 */
public class InterpretationRemarksDefinition {

  private static final Set<Term> TAXONOMY_TERMS =
          ImmutableSet.<Term>builder()
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

  private static final Set<Term> COORDINATES_TERMS =
          ImmutableSet.<Term>builder()
                  .add(DwcTerm.decimalLatitude)
                  .add(DwcTerm.decimalLongitude)
                  .add(DwcTerm.verbatimLatitude)
                  .add(DwcTerm.verbatimLatitude)
                  .add(DwcTerm.verbatimCoordinates)
                  .add(DwcTerm.geodeticDatum)
                  .build();

  /**
   * Mapping all OccurrenceIssue produced by the interpretation and its related data (severity, terms)
   */
  public static final BiMap<OccurrenceIssue, InterpretationRemark> REMARKS_MAP =
          ImmutableBiMap.<OccurrenceIssue, InterpretationRemark>builder()
                  // Errors
                  .put(OccurrenceIssue.BASIS_OF_RECORD_INVALID, InterpretationRemark.of(
                          OccurrenceIssue.BASIS_OF_RECORD_INVALID, InterpretationRemarkSeverity.ERROR,
                          DwcTerm.basisOfRecord))
                  .put(OccurrenceIssue.COORDINATE_OUT_OF_RANGE, InterpretationRemark.of(
                          OccurrenceIssue.COORDINATE_OUT_OF_RANGE, InterpretationRemarkSeverity.ERROR,
                          COORDINATES_TERMS))
                  .put(OccurrenceIssue.TAXON_MATCH_NONE, InterpretationRemark.of(
                          OccurrenceIssue.TAXON_MATCH_NONE, InterpretationRemarkSeverity.ERROR,
                          TAXONOMY_TERMS))
                  .put(OccurrenceIssue.INTERPRETATION_ERROR, InterpretationRemark.of(
                          OccurrenceIssue.INTERPRETATION_ERROR, InterpretationRemarkSeverity.ERROR))
                  // Warnings
                  .put(OccurrenceIssue.ZERO_COORDINATE, InterpretationRemark.of(
                          OccurrenceIssue.ZERO_COORDINATE, InterpretationRemarkSeverity.WARNING,
                          COORDINATES_TERMS))
                  .put(OccurrenceIssue.PRESUMED_SWAPPED_COORDINATE, InterpretationRemark.of(
                          OccurrenceIssue.PRESUMED_SWAPPED_COORDINATE, InterpretationRemarkSeverity.WARNING,
                          COORDINATES_TERMS))
                  .put(OccurrenceIssue.PRESUMED_SWAPPED_COORDINATE, InterpretationRemark.of(
                          OccurrenceIssue.PRESUMED_SWAPPED_COORDINATE, InterpretationRemarkSeverity.WARNING,
                          COORDINATES_TERMS))
                  .put(OccurrenceIssue.PRESUMED_NEGATED_LONGITUDE, InterpretationRemark.of(
                          OccurrenceIssue.PRESUMED_NEGATED_LONGITUDE, InterpretationRemarkSeverity.WARNING,
                          COORDINATES_TERMS))
                  .put(OccurrenceIssue.PRESUMED_NEGATED_LATITUDE, InterpretationRemark.of(
                          OccurrenceIssue.PRESUMED_NEGATED_LATITUDE, InterpretationRemarkSeverity.WARNING,
                          COORDINATES_TERMS))
                  .put(OccurrenceIssue.COUNTRY_INVALID, InterpretationRemark.of(
                          OccurrenceIssue.COUNTRY_INVALID, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.country, DwcTerm.countryCode))
                  .put(OccurrenceIssue.COUNTRY_MISMATCH, InterpretationRemark.of(
                          OccurrenceIssue.COUNTRY_MISMATCH, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.country, DwcTerm.countryCode))
                  .put(OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH, InterpretationRemark.of(
                          OccurrenceIssue.COUNTRY_COORDINATE_MISMATCH, InterpretationRemarkSeverity.WARNING,
                          ImmutableSet.<Term>builder().addAll(COORDINATES_TERMS).add(DwcTerm.country, DwcTerm.countryCode).build()))
                  .put(OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES, InterpretationRemark.of(
                          OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES, InterpretationRemarkSeverity.WARNING,
                          ImmutableSet.<Term>builder().addAll(COORDINATES_TERMS).add(DwcTerm.country, DwcTerm.countryCode).build()))
                  .put(OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84, InterpretationRemark.of(
                          OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.geodeticDatum))
                  .put(OccurrenceIssue.GEODETIC_DATUM_INVALID, InterpretationRemark.of(
                          OccurrenceIssue.GEODETIC_DATUM_INVALID, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.geodeticDatum))
                  .put(OccurrenceIssue.COORDINATE_REPROJECTION_FAILED, InterpretationRemark.of(
                          OccurrenceIssue.COORDINATE_REPROJECTION_FAILED, InterpretationRemarkSeverity.WARNING,
                          COORDINATES_TERMS))
                  .put(OccurrenceIssue.COORDINATE_REPROJECTION_SUSPICIOUS, InterpretationRemark.of(
                          OccurrenceIssue.COORDINATE_REPROJECTION_SUSPICIOUS, InterpretationRemarkSeverity.WARNING,
                          COORDINATES_TERMS))
                  .put(OccurrenceIssue.TAXON_MATCH_FUZZY, InterpretationRemark.of(
                          OccurrenceIssue.TAXON_MATCH_FUZZY, InterpretationRemarkSeverity.WARNING,
                          TAXONOMY_TERMS))
                  .put(OccurrenceIssue.TAXON_MATCH_HIGHERRANK, InterpretationRemark.of(
                          OccurrenceIssue.TAXON_MATCH_HIGHERRANK, InterpretationRemarkSeverity.WARNING,
                          TAXONOMY_TERMS))
                  .put(OccurrenceIssue.COORDINATE_PRECISION_INVALID, InterpretationRemark.of(
                          OccurrenceIssue.COORDINATE_PRECISION_INVALID, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.coordinatePrecision))
                  .put(OccurrenceIssue.COORDINATE_UNCERTAINTY_METERS_INVALID, InterpretationRemark.of(
                          OccurrenceIssue.COORDINATE_UNCERTAINTY_METERS_INVALID, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.coordinateUncertaintyInMeters))
                  .put(OccurrenceIssue.RECORDED_DATE_MISMATCH, InterpretationRemark.of(
                          OccurrenceIssue.RECORDED_DATE_MISMATCH, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.eventDate, DwcTerm.year, DwcTerm.month, DwcTerm.day))
                  .put(OccurrenceIssue.RECORDED_DATE_INVALID, InterpretationRemark.of(
                          OccurrenceIssue.RECORDED_DATE_INVALID, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.eventDate, DwcTerm.year, DwcTerm.month, DwcTerm.day))
                  .put(OccurrenceIssue.RECORDED_DATE_UNLIKELY, InterpretationRemark.of(
                          OccurrenceIssue.RECORDED_DATE_UNLIKELY, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.eventDate, DwcTerm.year, DwcTerm.month, DwcTerm.day))
                  .put(OccurrenceIssue.MODIFIED_DATE_INVALID, InterpretationRemark.of(
                          OccurrenceIssue.MODIFIED_DATE_INVALID, InterpretationRemarkSeverity.WARNING,
                          DcTerm.modified))
                  .put(OccurrenceIssue.MODIFIED_DATE_UNLIKELY, InterpretationRemark.of(
                          OccurrenceIssue.MODIFIED_DATE_UNLIKELY, InterpretationRemarkSeverity.WARNING,
                          DcTerm.modified))
                  .put(OccurrenceIssue.IDENTIFIED_DATE_UNLIKELY, InterpretationRemark.of(
                          OccurrenceIssue.IDENTIFIED_DATE_UNLIKELY, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.dateIdentified))
                  .put(OccurrenceIssue.IDENTIFIED_DATE_INVALID, InterpretationRemark.of(
                          OccurrenceIssue.IDENTIFIED_DATE_INVALID, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.dateIdentified))
                  .put(OccurrenceIssue.ELEVATION_NOT_METRIC, InterpretationRemark.of(
                          OccurrenceIssue.ELEVATION_NOT_METRIC, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.minimumElevationInMeters, DwcTerm.maximumElevationInMeters))
                  .put(OccurrenceIssue.ELEVATION_UNLIKELY, InterpretationRemark.of(
                          OccurrenceIssue.ELEVATION_UNLIKELY, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.minimumElevationInMeters, DwcTerm.maximumElevationInMeters))
                  .put(OccurrenceIssue.ELEVATION_MIN_MAX_SWAPPED, InterpretationRemark.of(
                          OccurrenceIssue.ELEVATION_MIN_MAX_SWAPPED, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.minimumElevationInMeters, DwcTerm.maximumElevationInMeters))
                  .put(OccurrenceIssue.ELEVATION_NON_NUMERIC, InterpretationRemark.of(
                          OccurrenceIssue.ELEVATION_NON_NUMERIC, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.minimumElevationInMeters, DwcTerm.maximumElevationInMeters))
                  .put(OccurrenceIssue.DEPTH_NOT_METRIC, InterpretationRemark.of(
                          OccurrenceIssue.DEPTH_NOT_METRIC, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.minimumDepthInMeters, DwcTerm.maximumDepthInMeters))
                  .put(OccurrenceIssue.DEPTH_UNLIKELY, InterpretationRemark.of(
                          OccurrenceIssue.DEPTH_UNLIKELY, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.minimumDepthInMeters, DwcTerm.maximumDepthInMeters))
                  .put(OccurrenceIssue.DEPTH_MIN_MAX_SWAPPED, InterpretationRemark.of(
                          OccurrenceIssue.DEPTH_MIN_MAX_SWAPPED, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.minimumDepthInMeters, DwcTerm.maximumDepthInMeters))
                  .put(OccurrenceIssue.DEPTH_NON_NUMERIC, InterpretationRemark.of(
                          OccurrenceIssue.DEPTH_NON_NUMERIC, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.minimumDepthInMeters, DwcTerm.maximumDepthInMeters))
                  .put(OccurrenceIssue.TYPE_STATUS_INVALID, InterpretationRemark.of(
                          OccurrenceIssue.TYPE_STATUS_INVALID, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.typeStatus))
                  .put(OccurrenceIssue.REFERENCES_URI_INVALID, InterpretationRemark.of(
                          OccurrenceIssue.REFERENCES_URI_INVALID, InterpretationRemarkSeverity.WARNING,
                          DcTerm.references))
                  .put(OccurrenceIssue.INDIVIDUAL_COUNT_INVALID, InterpretationRemark.of(
                          OccurrenceIssue.INDIVIDUAL_COUNT_INVALID, InterpretationRemarkSeverity.WARNING,
                          DwcTerm.individualCount))
                   // Provided in Multimedia extension so we can't link them to a specific Term
                   // We can also have multiple "media" but the error is attached to the Occurrence.
                   // In other words, we can't link the error to the problematic media.
                  .put(OccurrenceIssue.MULTIMEDIA_DATE_INVALID, InterpretationRemark.of(
                          OccurrenceIssue.MULTIMEDIA_DATE_INVALID, InterpretationRemarkSeverity.WARNING))
                  .put(OccurrenceIssue.MULTIMEDIA_URI_INVALID, InterpretationRemark.of(
                          OccurrenceIssue.MULTIMEDIA_URI_INVALID, InterpretationRemarkSeverity.WARNING))
                   // Info
                  .put(OccurrenceIssue.COORDINATE_ROUNDED, InterpretationRemark.of(
                          OccurrenceIssue.COORDINATE_ROUNDED, InterpretationRemarkSeverity.INFO,
                          DwcTerm.decimalLatitude, DwcTerm.decimalLongitude))
                  .put(OccurrenceIssue.COORDINATE_REPROJECTED, InterpretationRemark.of(
                          OccurrenceIssue.COORDINATE_REPROJECTED, InterpretationRemarkSeverity.INFO,
                          COORDINATES_TERMS))
                  .build();
  // Currently not used by occurrence-processor
//  COORDINATE_INVALID,
//  CONTINENT_INVALID,
//  CONTINENT_DERIVED_FROM_COORDINATES,
//  CONTINENT_COUNTRY_MISMATCH,
// ---------

  /**
   * a Set containing all remarks
   */
  public static final Set<InterpretationRemark> REMARKS = REMARKS_MAP.inverse().keySet();

  public static Set<Term> getRelatedTerms(OccurrenceIssue occIssue){
    return REMARKS_MAP.get(occIssue).getRelatedTerms();
  }


}
