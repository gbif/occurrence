/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.common;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GadmTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.IucnTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.predicate.query.SQLColumnsUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * This class serves to document the terms used in various stages of processing.  Please note that changes to this
 * class do not influence processing, although they define the formats for various Hive tables.
 *
 * <p>Processing is a complex procedure where, e.g. several verbatim fields are inspected, and depending on their content
 * will influence different fields in the interpreted view.  One example might be a verbatim view with dwc:eventDate
 * populated, but in the interpreted view dwc:eventDate, dwc:day, dwc:month and dwc:year are present.
 */
public class TermUtils {

  /**
   * The list of only the Dublin Core properties, excluding classes, used in Darwin Core or by GBIF.
   */
  private static final List<DcTerm> DwC_DC_PROPERTIES = dwcDcPropertyTerms();

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
   * The list of GADM properties applicable to occurrence records, excluding any classes and terms that are not
   * relevant to occurrence records.
   */
  private static final List<GadmTerm> GADM_PROPERTIES = gadmPropertyTerms();

  /**
   * The list of terms that are subject to interpretation and <strong>may</strong> not be present in the
   * interpreted record.  For example, dwc:maximumDepthInMeters may be present on a verbatim record, and subject to
   * interpretation, but (at the time of writing) is not be surfaced on the interpreted object but instead contributes
   * to the gbif:depth term.
   */
  private static final Set<Term> TERMS_SUBJECT_TO_INTERPRETATION = termsSubjectToInterpretation();

  /**
   * The map of term→value for terms that, after interpretation, have the same value for all occurrences.
   *
   * For example, coordinates are reprojected to WGS84, so dwc:geodeticDatum is "WGS84" for all occurrences.
   */
  private static final Map<Term,String> TERMS_IDENTICAL_AFTER_INTERPRETATION = termsIdenticalAfterInterpretation();

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
   * Utility to strip out classes and non-DwC non-GBIF terms from the complete Dublin Core enumeration.
   *
   * @return the complete list of property terms of Dublin Core used in Darwin Core or by GBIF.
   */
  private static List<DcTerm> dwcDcPropertyTerms() { // set to just dwc ones?
    // Note that DcTerm.identifier (perhaps unwisely) is used internally to hold the DwC-A id/coreId.
    // Its only purpose is to join the records within the archive, and is therefore not included here
    // as it has no meaning outside the archive.
    return ImmutableList.of(
      DcTerm.accessRights,
      DcTerm.bibliographicCitation,
      DcTerm.language,
      DcTerm.license,
      DcTerm.modified,
      DcTerm.publisher, // Not DwC, but used by GBIF
      DcTerm.references,
      DcTerm.rightsHolder,
      DcTerm.type
    );
  }

  /**
   * Utility to strip out classes from the GBIF enumeration.
   *
   * @return the complete list of property terms of the GBIF namespace, excluding any "class" terms and terms not
   * relevant to occurrences.
   */
  private static List<GbifTerm> gbifPropertyTerms() {
    final Set<GbifTerm> exclusions = ImmutableSet.of(
      // the following have no place on occurrence
      GbifTerm.infraspecificMarker,
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

      // And these have been superseded by other terms or otherwise deprecated and removed
      GbifTerm.distanceAboveSurface,
      GbifTerm.distanceAboveSurfaceAccuracy
      );

    return Arrays.stream(GbifTerm.values()).filter(t -> !t.isClass() && !exclusions.contains(t))
      .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));
  }

  /**
   * Utility to strip out classes from the complete Gadm enumeration.
   *
   * @return the complete list of property terms of Gadm, excluding any "class" terms (though there are none yet)
   */
  private static List<GadmTerm> gadmPropertyTerms() {
    return Arrays.stream(GadmTerm.values()).filter(t -> !t.isClass())
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
   * internally generated.  These are all explicit Java properties on the
   * {@link org.gbif.api.model.occurrence.Occurrence} class.
   *
   * @return the terms with values that will only be populated following some interpretation
   */
  private static Set<Term> termsPopulatedByInterpretation() {
    return ImmutableSet.of(
      DwcTerm.decimalLatitude,
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
      DwcTerm.startDayOfYear,
      DwcTerm.endDayOfYear,
      DwcTerm.kingdom,
      DwcTerm.phylum,
      DwcTerm.class_,
      DwcTerm.order,
      DwcTerm.family,
      DwcTerm.genus,
      DwcTerm.subgenus,
      GbifTerm.species,
      DwcTerm.scientificName,
      DwcTerm.taxonRank,
      DwcTerm.taxonomicStatus,
      GbifTerm.acceptedScientificName,
      DwcTerm.genericName,
      DwcTerm.specificEpithet,
      DwcTerm.infraspecificEpithet,
      DwcTerm.basisOfRecord,
      DwcTerm.individualCount,
      DwcTerm.sex,
      DwcTerm.lifeStage,
      DwcTerm.establishmentMeans,
      DwcTerm.pathway,
      DwcTerm.degreeOfEstablishment,
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
      GbifTerm.distanceFromCentroidInMeters,
      GbifTerm.depth,
      GbifTerm.depthAccuracy,
      GadmTerm.level0Gid,
      GadmTerm.level0Name,
      GadmTerm.level1Gid,
      GadmTerm.level1Name,
      GadmTerm.level2Gid,
      GadmTerm.level2Name,
      GadmTerm.level3Gid,
      GadmTerm.level3Name,
      DwcTerm.sampleSizeUnit,
      DwcTerm.sampleSizeValue,
      DwcTerm.organismQuantityType,
      DwcTerm.organismQuantity,
      GbifInternalTerm.unitQualifier,
      GbifTerm.issue,
      DwcTerm.recordedByID,
      DwcTerm.identifiedByID,
      DcTerm.references,
      GbifTerm.protocol,
      GbifTerm.lastCrawled,
      GbifTerm.lastParsed,
      GbifInternalTerm.installationKey,
      GbifInternalTerm.publishingOrgKey,
      GbifInternalTerm.networkKey,
      GbifTerm.mediaType,
      DcTerm.license,
      DwcTerm.occurrenceStatus,
      DwcTerm.datasetID,
      DwcTerm.datasetName,
      DwcTerm.otherCatalogNumbers,
      DwcTerm.recordedBy,
      DwcTerm.identifiedBy,
      DwcTerm.preparations,
      DwcTerm.samplingProtocol,
      GbifTerm.isSequenced,
      GbifTerm.gbifRegion,
      GbifTerm.publishedByGbifRegion,
      DwcTerm.georeferencedBy,
      DwcTerm.higherGeography,
      DwcTerm.earliestEonOrLowestEonothem,
      DwcTerm.latestEonOrHighestEonothem,
      DwcTerm.earliestEraOrLowestErathem,
      DwcTerm.latestEraOrHighestErathem,
      DwcTerm.earliestPeriodOrLowestSystem,
      DwcTerm.latestPeriodOrHighestSystem,
      DwcTerm.earliestEpochOrLowestSeries,
      DwcTerm.latestEpochOrHighestSeries,
      DwcTerm.earliestAgeOrLowestStage,
      DwcTerm.latestAgeOrHighestStage,
      DwcTerm.lowestBiostratigraphicZone,
      DwcTerm.highestBiostratigraphicZone,
      DwcTerm.associatedSequences,
      DwcTerm.group,
      DwcTerm.formation,
      DwcTerm.member,
      DwcTerm.bed,
      GbifTerm.projectId);
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
      .add(
        DwcTerm.decimalLatitude,
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
        DwcTerm.genericName,
        DwcTerm.specificEpithet,
        DwcTerm.infraspecificEpithet,
        DcTerm.modified,
        DwcTerm.dateIdentified,
        DwcTerm.eventDate,
        DwcTerm.year,
        DwcTerm.month,
        DwcTerm.day,
        DwcTerm.startDayOfYear,
        DwcTerm.endDayOfYear,
        DwcTerm.minimumDepthInMeters,
        DwcTerm.maximumDepthInMeters,
        DwcTerm.minimumElevationInMeters,
        DwcTerm.maximumElevationInMeters,
        DwcTerm.associatedMedia)
      .build();
  }

  /**
   * Lists all terms that, after interpretation, hold the same value, and that value.
   *
   * @return a map of term→value for term that, due to standardization, now have identical values on all records.
   */
  private static Map<Term,String> termsIdenticalAfterInterpretation() {
    return ImmutableMap.<Term,String>builder()
      .put(DwcTerm.geodeticDatum, Occurrence.GEO_DATUM)
      .build();
  }

  /**
   * Term list of the extension excluding the coreid just as defined by:
   * <a href="http://rs.gbif.org/terms/1.0/Multimedia">the multimedia extension</a>
   */
  private static final List<DcTerm> MULTIMEDIA_TERMS = ImmutableList.of(DcTerm.type,
                                                                        DcTerm.format,
                                                                        DcTerm.identifier,
                                                                        DcTerm.references,
                                                                        DcTerm.title,
                                                                        DcTerm.description,
                                                                        DcTerm.source,
                                                                        DcTerm.audience,
                                                                        DcTerm.created,
                                                                        DcTerm.creator,
                                                                        DcTerm.contributor,
                                                                        DcTerm.publisher,
                                                                        DcTerm.license,
                                                                        DcTerm.rightsHolder);

  private TermUtils() {
    // private constructor
  }

  /**
   * Lists all terms that have been used during interpretation and are superseded by an interpreted,
   * typed java Occurrence property.
   *
   * @return iterable of terms that have been used during interpretation
   */
  public static Iterable<? extends Term> interpretedSourceTerms() {
    return TERMS_SUBJECT_TO_INTERPRETATION;
  }

  /**
   * @return true if the term is used during interpretation and superseded by an interpreted property
   */
  public static boolean isInterpretedSourceTerm(Term term) {
    return TERMS_SUBJECT_TO_INTERPRETATION.contains(term);
  }

  /**
   * Returns the list of all terms which can be present in the verbatim view of an Occurrence.  This is defined as:
   * <ul>
   * <li>The GBIF ID</li>
   * <li>The Dublin Core terms that are part of Darwin Core (and excluding potential "class" terms)</li>
   * <li>The Darwin Core terms excluding "class" terms and excluding any not suitable for occurrence records</li>
   * </ul>
   */
  public static List<Term> verbatimTerms() {
    return ImmutableList.<Term>builder()
      .add(GbifTerm.gbifID)
      .addAll(DwC_DC_PROPERTIES)
      .addAll(DwC_PROPERTIES)
      .build();
  }

  /**
   * Returns the list of all terms which can be present in the interpreted view of an Occurrence.  This is defined as
   * <ul>
   * <li>The GBIF ID</li>
   * <li>DwC-DC terms that are not removed during interpretation</li>
   * <li>DwC terms that are not removed during interpretation</li>
   * <li>The remaining GBIF terms not removed during interpretation and not deprecated</li>
   * </ul>
   */
  public static List<Term> interpretedTerms() {
    return ImmutableList.<Term>builder().add(GbifTerm.gbifID)
      .addAll(
        // add all Dublin Core terms that are not stripped during interpretation
        DwC_DC_PROPERTIES.stream().filter(t -> !TERMS_REMOVED_DURING_INTERPRETATION.contains(t)).collect(Collectors.toList()))
      .addAll(
        // add all Darwin Core terms that are not stripped during interpretation
        DwC_PROPERTIES.stream().filter(t -> !TERMS_REMOVED_DURING_INTERPRETATION.contains(t)).collect(Collectors.toList()))
      .addAll(
        // add all GBIF terms that are not stripped during interpretation
        GBIF_PROPERTIES.stream().filter(t -> !TERMS_REMOVED_DURING_INTERPRETATION.contains(t) && GbifTerm.gbifID != t
          && GbifTerm.coordinateAccuracy != t && GbifTerm.numberOfOccurrences != t).collect(Collectors.toList()))
      .addAll(
        // add all GADM terms (none are stripped during interpretation, but filter anyway).
        GADM_PROPERTIES.stream().filter(t -> !TERMS_REMOVED_DURING_INTERPRETATION.contains(t)).collect(Collectors.toList()))
      //IUCN RedList Category
      .add(IucnTerm.iucnRedListCategory)
      .build();
  }

  /**
   * Returns the map of term→value for all terms which, after interpretation, have the same value on all occurrences.
   * In Darwin Core Archive terms, this is a default value.
   */
  public static Map<Term,String> identicalInterpretedTerms() {
    return TERMS_IDENTICAL_AFTER_INTERPRETATION;
  }

  /**
   * Lists all terms relevant for a multimedia extension record.
   * gbifID is included and comes first as it is the foreign key to the core record.
   */
  public static Iterable<Term> multimediaTerms() {
    return Iterables.concat(Collections.singletonList(GbifTerm.gbifID), MULTIMEDIA_TERMS);
  }

  /**
   * @return true if the term is an interpreted local date (timezone not relevant)
   */
  public static boolean isInterpretedLocalDateSeconds(Term term) {
    return SQLColumnsUtils.isInterpretedLocalDateSeconds(term);
  }

  /**
   * @return true if the term is an interpreted UTC date with
   */
  public static boolean isInterpretedUtcDateSeconds(Term term) {
    return SQLColumnsUtils.isInterpretedUtcDateSeconds(term);
  }

  /**
   * @return true if the term is an interpreted UTC date with
   */
  public static boolean isInterpretedUtcDateMilliseconds(Term term) {
    return SQLColumnsUtils.isInterpretedUtcDateMilliseconds(term);
  }

  /**
   * @return true if the term is an interpreted numerical
   */
  public static boolean isInterpretedNumerical(Term term) {
    return SQLColumnsUtils.isInterpretedNumerical(term);
  }

  /**
   * @return true if the term is an interpreted double
   */
  public static boolean isInterpretedDouble(Term term) {
    return SQLColumnsUtils.isInterpretedDouble(term);
  }

  /**
   * @return true if the term is an interpreted boolean
   */
  public static boolean isInterpretedBoolean(Term term) {
    return SQLColumnsUtils.isInterpretedBoolean(term);
  }

  /**
   * @return true if the term is a complex type in Hive: array, struct, json, etc.
   */
  public static boolean isComplexType(Term term) {
    return SQLColumnsUtils.isComplexType(term);
  }

  public static boolean isExtensionTerm(Term term) {
    return SQLColumnsUtils.isExtensionTerm(term);
  }

  /**
   * @return true if the term is a handled/annotated as Vocabulary.
   */
  public static boolean isVocabulary(Term term) {
    return SQLColumnsUtils.isVocabulary(term);
  }

  /**
   * @return true if the term is a handled/annotated as an array.
   */
  public static boolean isArray(Term term) {
    return SQLColumnsUtils.isSQLArray(term);
  }
}
