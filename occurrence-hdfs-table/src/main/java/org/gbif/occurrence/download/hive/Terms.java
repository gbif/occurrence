package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * This class serves to document the terms used in various stages of processing.  Please note that changes to this
 * class do not influence processing, although they define the formats for various Hive tables.
 * <p/>
 * Processing is complex procedure where, e.g. several verbatim fields are inspected, and depending on their content
 * will influence different fields in the interpreted view.  One example might be a verbatim view with dwc:eventDate
 * populated, but in the interpreted view dwc:eventDate, dwc:day, dwc:month and dwc:year are present.
 * <p/>
 * The code in this class is intended as a more intuitive replacement for {@link org.gbif.occurrence.common.TermUtils}
 * and should be merged into that class when ready.
 */
public final class Terms {

  /**
   * The list of only the Dublin Core properties, excluding classes, such as Location.
   */
  private static final List<DcTerm> DC_PROPERTIES = dcPropertyTerms();

  /**
   * The list of Darwin Core properties applicable to occurrence records, excluding classes such as Taxon and terms
   * that are not relevant to occurrence records.
   */
  private static final List<DwcTerm> DwC_PROPERTIES = dwcPropertyTerms();

  /**
   * The list of GBIF properties applicable to occurrence records, excluding any classes and terms that are not
   * relevant to occurrence records.
   */
  private static final List<GbifTerm> GBIF_PROPERTIES = gbifPropertyTerms();

  /**
   * The list of terms that are subject to interpretation and <strong>may</strong> not be present in the
   * interpreted record.  For example, dwc:maximumDepthInMeters may be present on a verbatim record, and subject to
   * interpretation, but (at the time of writing) is not be surfaced on the interpreted object but instead contributes
   * to the gbif:depth term.
   */
  private static final Set<Term> TERMS_SUBJECT_TO_INTERPRETATION = termsSubjectToInterpretation();

  /**
   * The terms that are present only due to explicit interpretation.  These are often typed explicitly, such as Dates
   * or are the result of a routine that has analyzed various verbatim fields and interpreted them into new values,
   * such as the dwc:kingdom ... dwc:scientificName fields which are subject to a nub lookup.
   */
  private static final Set<Term> TERMS_POPULATED_BY_INTERPRETATION = termsPopulatedByInterpretation();

  /**
   * The terms that are subject to interpretation but not present on the interpreted occurrence.
   */
  private static final Set<Term> TERMS_REMOVED_DURING_INTERPRETATION =
    Sets.difference(TERMS_SUBJECT_TO_INTERPRETATION, TERMS_POPULATED_BY_INTERPRETATION);

  /**
   * Utility to strip out classes from the complete Dublin Core enumeration.
   *
   * @return the complete list of property terms of Dublin Core, excluding any "class" terms such as Location.
   */
  private static List<DcTerm> dcPropertyTerms() {
    return Arrays.stream(DcTerm.values()).filter(t -> !t.isClass())
              .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
  }

  /**
   * Utility to strip out classes from the GBIF enumeration.
   *
   * @return the complete list of property terms of the GBIF namespace, excluding any "class" terms and terms not
   * relevant to occurrences.
   */
  private static List<GbifTerm> gbifPropertyTerms() {
    // the following have no place on occurrence
    final Set<GbifTerm> exclusions = ImmutableSet.of(GbifTerm.infraspecificMarker,
                                                     GbifTerm.isExtinct,
                                                     GbifTerm.isFreshwater,
                                                     GbifTerm.isHybrid,
                                                     GbifTerm.isMarine,
                                                     GbifTerm.isPlural,
                                                     GbifTerm.isPreferredName,
                                                     GbifTerm.isTerrestrial,
                                                     GbifTerm.livingPeriod,
                                                     GbifTerm.lifeForm,
                                                     GbifTerm.ageInDays,
                                                     GbifTerm.sizeInMillimeter,
                                                     GbifTerm.massInGram,
                                                     GbifTerm.organismPart,
                                                     GbifTerm.appendixCITES,
                                                     GbifTerm.typeDesignatedBy,
                                                     GbifTerm.typeDesignationType,
                                                     GbifTerm.canonicalName,
                                                     GbifTerm.nameType,
                                                     GbifTerm.verbatimLabel,
                                                     GbifTerm.infraspecificMarker);

    //We should handle deprecated terms here. Waiting for GBIF-132
    return Arrays.stream(GbifTerm.values()).filter(t -> !t.isClass() && !exclusions.contains(t))
            .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
  }

  /**
   * Utility to strip out all classes from the DwC terms and all properties that are not applicable to an occurrence
   * record.
   *
   * @return the complete list of property terms of Darwin Core, excluding any "class" terms such as Taxon and terms
   * not relevant to occurrence records.
   */
  private static List<DwcTerm> dwcPropertyTerms() {
    // the following are only used in extensions
    final Set<DwcTerm> exclusions = ImmutableSet.<DwcTerm>builder()
      .addAll(DwcTerm.listByGroup(DwcTerm.GROUP_MEASUREMENTORFACT))
      .addAll(DwcTerm.listByGroup(DwcTerm.GROUP_RESOURCERELATIONSHIP))
      .build();
    return Arrays.stream(DwcTerm.values()).filter(t ->   !t.isClass() && !exclusions.contains(t))
      .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
  }

  /**
   * Lists all the terms which are populated on the occurrence object by interpretation, explicit processing or are
   * internally generated.  These are all explicit java properties on the
   * {@link org.gbif.api.model.occurrence.Occurrence} class.
   *
   * @return the terms with values that will only be populated following some interpretation
   */
  private static Set<Term> termsPopulatedByInterpretation() {
    return ImmutableSet.of(DwcTerm.decimalLatitude,
                           DwcTerm.decimalLongitude,
                           DwcTerm.continent,
                           DwcTerm.waterBody,
                           DwcTerm.stateProvince,
                           DwcTerm.countryCode,
                           DwcTerm.dateIdentified,
                           DwcTerm.eventDate,
                           DwcTerm.year,
                           DwcTerm.month,
                           DwcTerm.day,
                           DwcTerm.kingdom,
                           DwcTerm.phylum,
                           DwcTerm.class_,
                           DwcTerm.order,
                           DwcTerm.family,
                           DwcTerm.genus,
                           DwcTerm.subgenus,
                           GbifTerm.species,
                           DwcTerm.scientificName,
                           GbifTerm.acceptedScientificName,
                           DwcTerm.taxonRank,
                           DwcTerm.taxonomicStatus,
                           // DwcTerm.verbatimCoordinates,
                           GbifTerm.genericName,
                           DwcTerm.specificEpithet,
                           DwcTerm.infraspecificEpithet,
                           DwcTerm.basisOfRecord,
                           DwcTerm.individualCount,
                           DwcTerm.sex,
                           DwcTerm.lifeStage,
                           DwcTerm.establishmentMeans,
                           GbifTerm.taxonKey,
                           GbifTerm.acceptedTaxonKey,
                           DwcTerm.typeStatus,
                           GbifTerm.typifiedName,
                           GbifTerm.kingdomKey,
                           GbifTerm.phylumKey,
                           GbifTerm.classKey,
                           GbifTerm.orderKey,
                           GbifTerm.familyKey,
                           GbifTerm.genusKey,
                           GbifTerm.subgenusKey,
                           GbifTerm.speciesKey,
                           GbifTerm.datasetKey,
                           GbifTerm.publishingCountry,
                           GbifTerm.lastInterpreted,
                           DcTerm.modified,
                           DwcTerm.coordinateUncertaintyInMeters,
                           DwcTerm.coordinatePrecision,
                           GbifTerm.elevation,
                           GbifTerm.elevationAccuracy,
                           GbifTerm.depth,
                           GbifTerm.depthAccuracy,
                           GbifInternalTerm.unitQualifier,
                           GbifTerm.issue,
                           DcTerm.references,
                           GbifTerm.datasetKey,
                           GbifTerm.publishingCountry,
                           GbifTerm.protocol,
                           GbifTerm.lastCrawled,
                           GbifTerm.lastParsed,
                           GbifInternalTerm.installationKey,
                           GbifInternalTerm.publishingOrgKey,
                           GbifInternalTerm.networkKey);
  }

  /**
   * Lists all terms that are subject to interpretation.  Some of the terms may be present on the interpreted
   * occurrence record, but others will not.  This simply lists those that will be interpreted.
   *
   * @return the set of terms that will be processed by interpretation routines, and may disappear from the record
   */
  private static Set<Term> termsSubjectToInterpretation() {
    // any term that is populated by interpretation has to be subject to interpretation if present on the
    // verbatim record
    return ImmutableSet.<Term>builder()
      .addAll(termsPopulatedByInterpretation())
      .add(DwcTerm.decimalLatitude,
           DwcTerm.decimalLongitude,
           DwcTerm.verbatimLatitude,
           DwcTerm.verbatimLongitude,
           DwcTerm.verbatimCoordinates,
           DwcTerm.geodeticDatum,
           DwcTerm.coordinateUncertaintyInMeters,
           DwcTerm.coordinatePrecision,
           DwcTerm.continent,
           DwcTerm.waterBody,
           DwcTerm.stateProvince,
           DwcTerm.country,
           DwcTerm.countryCode,
           DwcTerm.scientificName,
           DwcTerm.scientificNameAuthorship,
           DwcTerm.taxonRank,
           DwcTerm.taxonomicStatus,
           DwcTerm.kingdom,
           DwcTerm.phylum,
           DwcTerm.class_,
           DwcTerm.order,
           DwcTerm.family,
           DwcTerm.genus,
           DwcTerm.subgenus,
           GbifTerm.genericName,
           DwcTerm.specificEpithet,
           DwcTerm.infraspecificEpithet,
           DcTerm.modified,
           DwcTerm.dateIdentified,
           DwcTerm.eventDate,
           DwcTerm.year,
           DwcTerm.month,
           DwcTerm.day,
           DwcTerm.minimumDepthInMeters,
           DwcTerm.maximumDepthInMeters,
           DwcTerm.minimumElevationInMeters,
           DwcTerm.maximumElevationInMeters,
           DwcTerm.associatedMedia)
      .build();
  }

  /**
   * Returns the list of all terms which can be present in the verbatim view of an Occurrence.  This is defined as:
   * <ul>
   * <li>The GBIF ID</li>
   * <li>The complete Dublin Core terms excluding "class" terms</li>
   * <li>The Darwin Core terms excluding "class" terms and any not suitable for occurrence records</li>
   * </ul>
   */
  public static List<Term> verbatimTerms() {
    return ImmutableList.<Term>builder().add(GbifTerm.gbifID).addAll(DC_PROPERTIES).addAll(DwC_PROPERTIES).build();
  }

  /**
   * Returns the list of all terms which can be present in the interpreted view of an Occurrence.  This is defined as
   * <ul>
   * <li>The GBIF ID</li>
   * <li>DC terms that are not removed during interpretation</li>
   * <li>DwC terms that are not removed during interpretation</li>
   * <li>The remaining GBIF terms not removed during interpretation and not deprecated</li>
   * </ul>
   */
    public static List<Term> interpretedTerms() {

    return ImmutableList.<Term>builder().add(GbifTerm.gbifID).addAll(
      // add all Dublin Core terms that are not stripped during interpretation
      DC_PROPERTIES.stream().filter(t -> !TERMS_REMOVED_DURING_INTERPRETATION.contains(t)).collect(Collectors.toList())
      ).addAll(
      // add all Darwin Core terms that are not stripped during interpretation
      DwC_PROPERTIES.stream().filter(t -> !TERMS_REMOVED_DURING_INTERPRETATION.contains(t)).collect(Collectors.toList()))
      .addAll(
      // add all GBIF terms that are not stripped during interpretation\
      GBIF_PROPERTIES.stream().filter(t -> !TERMS_REMOVED_DURING_INTERPRETATION.contains(t) && GbifTerm.gbifID != t
        && GbifTerm.coordinateAccuracy !=t && GbifTerm.numberOfOccurrences != t).collect(Collectors.toList()))
      .build();
  }

  private Terms() {
    // empty constructor
  }
}
