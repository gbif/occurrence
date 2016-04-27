package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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
    return ImmutableList.copyOf(Iterables.<DcTerm>filter(Lists.newArrayList(DcTerm.values()), new Predicate<DcTerm>() {
                                                           @Override
                                                           public boolean apply(DcTerm t) {
                                                             return !t.isClass();
                                                           }
                                                         }));
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

    return ImmutableList.copyOf(Iterables.<GbifTerm>filter(Lists.newArrayList(GbifTerm.values()),
                                                           new Predicate<GbifTerm>() {
                                                             @Override
                                                             public boolean apply(GbifTerm t) {
                                                               return !t.isClass() && !exclusions.contains(t);
                                                             }
                                                           }));
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

    return ImmutableList.copyOf(Iterables.<DwcTerm>filter(
      // Remove any that are "classes", or explicitly omitted
      Lists.newArrayList(DwcTerm.values()), new Predicate<DwcTerm>() {
        @Override
        public boolean apply(DwcTerm t) {
          return !t.isClass() && !exclusions.contains(t);
        }
      }));
  }

  /**
   * Lists all the terms which are populated on the occurrence object by interpretation, explicit processing or are
   * internally generated.  These are all explicit java properties on the
   * {@link org.gbif.api.model.occurrence.Occurrence} class.
   *
   * @return the terms with values that will only be populated following some interpretation
   */
  private static Set<Term> termsPopulatedByInterpretation() {
    return ImmutableSet.<Term>of(DwcTerm.decimalLatitude,
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
                                 DwcTerm.taxonRank,
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
                                 GbifTerm.lastParsed);
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
   * <li>The remaining GBIF terms not removed during interpretation</li>
   * </ul>
   */
  public static List<Term> interpretedTerms() {
    return ImmutableList.<Term>builder().add(GbifTerm.gbifID).addAll(
      // add all Dublin Core terms that are not stripped during interpretation
      Iterables.filter(DC_PROPERTIES, new Predicate<Term>() {
        @Override
        public boolean apply(@Nullable Term t) {
          return !TERMS_REMOVED_DURING_INTERPRETATION.contains(t);
        }
      })).addAll(
      // add all Darwin Core terms that are not stripped during interpretation
      Iterables.filter(DwC_PROPERTIES, new Predicate<Term>() {
        @Override
        public boolean apply(@Nullable Term t) {
          return !TERMS_REMOVED_DURING_INTERPRETATION.contains(t);
        }
      })).addAll(
      // add all GBIF terms that are not stripped during interpretation
      Iterables.filter(GBIF_PROPERTIES, new Predicate<Term>() {
        @Override
        public boolean apply(@Nullable Term t) {
          // strip the GBIF id as we've already added that
          return !TERMS_REMOVED_DURING_INTERPRETATION.contains(t) && GbifTerm.gbifID != t;
        }
      })).build();
  }

  private Terms() {
    // empty constructor
  }
}
