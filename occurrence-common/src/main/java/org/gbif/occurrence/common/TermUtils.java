package org.gbif.occurrence.common;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Static utils class to deal with Term enumeration for occurrences.
 */
public class TermUtils {

  private static final ImmutableSet<String> HIVE_RESERVED_WORDS = new ImmutableSet.Builder<String>().add("date",
    "order", "format", "group").build();

  private static final Set<? extends Term> INTERPRETED_DATES = ImmutableSet.of(
    DwcTerm.eventDate, DwcTerm.dateIdentified, GbifTerm.lastInterpreted, GbifTerm.lastParsed, GbifTerm.lastCrawled,
    DcTerm.modified, GbifInternalTerm.fragmentCreated);

  private static final Set<? extends Term> INTERPRETED_NUM = ImmutableSet.of(
    DwcTerm.year, DwcTerm.month, DwcTerm.day, GbifTerm.taxonKey, GbifTerm.kingdomKey, GbifTerm.phylumKey,
    GbifTerm.classKey, GbifTerm.orderKey, GbifTerm.familyKey,
    GbifTerm.genusKey, GbifTerm.subgenusKey, GbifTerm.speciesKey,
    GbifInternalTerm.crawlId, GbifInternalTerm.identifierCount);

  private static final Set<? extends Term> INTERPRETED_BOOLEAN = ImmutableSet.of(GbifTerm.hasCoordinate,
    GbifTerm.hasGeospatialIssues);

  private static final Set<? extends Term> INTERPRETED_DOUBLE = ImmutableSet.of(
    DwcTerm.decimalLatitude, DwcTerm.decimalLongitude, GbifTerm.coordinateAccuracy,
    GbifTerm.elevation, GbifTerm.elevationAccuracy, GbifTerm.depth, GbifTerm.depthAccuracy);

  private static final Set<? extends Term> NON_OCCURRENCE_TERMS = (Set<? extends Term>) ImmutableSet.copyOf(
    Iterables.concat(DwcTerm.listByGroup(DwcTerm.GROUP_MEASUREMENTORFACT),
      DwcTerm.listByGroup(DwcTerm.GROUP_RESOURCERELATIONSHIP),
      Sets.newHashSet(GbifTerm.infraspecificMarker, GbifTerm.isExtinct, GbifTerm.isFreshwater,
        GbifTerm.isHybrid, GbifTerm.isMarine, GbifTerm.isPlural, GbifTerm.isPreferredName,
        GbifTerm.isTerrestrial, GbifTerm.livingPeriod, GbifTerm.lifeForm, GbifTerm.ageInDays,
        GbifTerm.sizeInMillimeter, GbifTerm.massInGram, GbifTerm.organismPart,
        GbifTerm.appendixCITES, GbifTerm.typeDesignatedBy, GbifTerm.typeDesignationType,
        GbifTerm.canonicalName, GbifTerm.nameType, GbifTerm.verbatimLabel,
        GbifTerm.infraspecificMarker)
      )
    );

  /**
   * Interpreted terms that exist as java properties on Occurrence.
   */
  private static final Set<? extends Term> JAVA_PROPERTY_TERMS = ImmutableSet.of(
    DwcTerm.decimalLatitude, DwcTerm.decimalLongitude,
    DwcTerm.continent, DwcTerm.waterBody, DwcTerm.stateProvince, DwcTerm.countryCode,
    DwcTerm.dateIdentified, DwcTerm.eventDate, DwcTerm.year, DwcTerm.month, DwcTerm.day,
    DwcTerm.kingdom, DwcTerm.phylum, DwcTerm.class_, DwcTerm.order, DwcTerm.family, DwcTerm.genus, DwcTerm.subgenus,
    GbifTerm.species, DwcTerm.scientificName, DwcTerm.taxonRank,
    DwcTerm.genericName, DwcTerm.specificEpithet, DwcTerm.infraspecificEpithet,
    DwcTerm.basisOfRecord, DwcTerm.individualCount, DwcTerm.sex, DwcTerm.lifeStage, DwcTerm.establishmentMeans,
    GbifTerm.taxonKey, DwcTerm.typeStatus, DwcTerm.typifiedName,
    GbifTerm.kingdomKey, GbifTerm.phylumKey, GbifTerm.classKey, GbifTerm.orderKey, GbifTerm.familyKey,
    GbifTerm.genusKey, GbifTerm.subgenusKey, GbifTerm.speciesKey,
    GbifTerm.datasetKey, GbifTerm.publishingCountry,
    GbifTerm.lastInterpreted, DcTerm.modified,
    GbifTerm.coordinateAccuracy,
    GbifTerm.elevation, GbifTerm.elevationAccuracy,
    GbifTerm.depth, GbifTerm.depthAccuracy,
    GbifInternalTerm.unitQualifier, GbifTerm.issue,
    GbifTerm.datasetKey, GbifTerm.publishingCountry, GbifTerm.protocol, GbifTerm.lastCrawled, GbifTerm.lastParsed
    );


  private static final Set<? extends Term> INTERPRETED_SOURCE_TERMS = (Set<? extends Term>) ImmutableSet.copyOf(
    Iterables.concat(JAVA_PROPERTY_TERMS,
      Lists.newArrayList(
        DwcTerm.decimalLatitude, DwcTerm.decimalLongitude,
        DwcTerm.verbatimLatitude, DwcTerm.verbatimLongitude,
        DwcTerm.coordinateUncertaintyInMeters, DwcTerm.coordinatePrecision,
        DwcTerm.continent, DwcTerm.waterBody, DwcTerm.stateProvince, DwcTerm.country, DwcTerm.countryCode,
        DwcTerm.scientificName, DwcTerm.scientificNameAuthorship, DwcTerm.taxonRank,
        DwcTerm.kingdom, DwcTerm.phylum, DwcTerm.class_, DwcTerm.order, DwcTerm.family, DwcTerm.genus,
        DwcTerm.subgenus,
        DwcTerm.genericName, DwcTerm.specificEpithet, DwcTerm.infraspecificEpithet,
        DcTerm.modified, DwcTerm.dateIdentified, DwcTerm.eventDate, DwcTerm.year, DwcTerm.month, DwcTerm.day,
        DwcTerm.minimumDepthInMeters, DwcTerm.maximumDepthInMeters,
        DwcTerm.minimumElevationInMeters, DwcTerm.maximumElevationInMeters
        )
      )
    );

  private TermUtils() {
  }

  /**
   * Gets the Hive column name of the term parameter.
   */
  public static String getHiveColumn(Term term) {
    final String columnName = term.simpleName().toLowerCase();
    if (HIVE_RESERVED_WORDS.contains(columnName)) {
      return columnName + '_';
    }
    return columnName;
  }


  /**
   * Returns the Hive data type of term parameter.
   */
  public static String getHiveType(Term term) {
    if (TermUtils.isInterpretedNumerical(term)) {
      return "INT";
    } else if (TermUtils.isInterpretedDate(term)) {
      return "BIGINT";
    } else if (TermUtils.isInterpretedDouble(term)) {
      return "DOUBLE";
    } else if (TermUtils.isInterpretedBoolean(term)) {
      return "BOOLEAN";
    } else {
      return "STRING";
    }
  }

  /**
   * Gets the Hive column name of the term parameter.
   */
  public static String getHiveColumn(OccurrenceIssue issue) {
    final String columnName = issue.name().toLowerCase();
    if (HIVE_RESERVED_WORDS.contains(columnName)) {
      return columnName + '_';
    }
    return columnName;
  }

  /**
   * Lists all terms that have been used during interpretation and are superseded by an interpreted,
   * typed java Occurrence property.
   * 
   * @return iterable of terms that have been used during interpretation
   */
  public static Iterable<? extends Term> interpretedSourceTerms() {
    return INTERPRETED_SOURCE_TERMS;
  }

  /**
   * @return true if the term is used during interpretation and superseded by an interpreted property
   */
  public static boolean isInterpretedSourceTerm(Term term) {
    return INTERPRETED_SOURCE_TERMS.contains(term);
  }

  /**
   * @return true if the term is an interpreted value stored as a java property on Occurrence.
   */
  public static boolean isOccurrenceJavaProperty(Term term) {
    return JAVA_PROPERTY_TERMS.contains(term);
  }

  /**
   * Lists all terms relevant for an interpreted occurrence record, starting with occurrenceID as the key.
   * UnknownTerms are not included as they are open ended.
   */
  public static Iterable<? extends Term> interpretedTerms() {
    return Iterables.concat(
      Lists.newArrayList(GbifTerm.gbifID),
      Iterables.filter(Lists.newArrayList(DcTerm.values()), new Predicate<DcTerm>() {

        @Override
        public boolean apply(@Nullable DcTerm t) {
          return !t.isClass()
            && (!INTERPRETED_SOURCE_TERMS.contains(t) || JAVA_PROPERTY_TERMS.contains(t));
        }
      }), Iterables.filter(Lists.newArrayList(DwcTerm.values()), new Predicate<DwcTerm>() {

        @Override
        public boolean apply(@Nullable DwcTerm t) {
          return !t.isClass() && !NON_OCCURRENCE_TERMS.contains(t)
            && (!INTERPRETED_SOURCE_TERMS.contains(t) || JAVA_PROPERTY_TERMS.contains(t));
        }
      }), Iterables.filter(Lists.newArrayList(GbifTerm.values()), new Predicate<GbifTerm>() {

        @Override
        public boolean apply(@Nullable GbifTerm t) {
          return !t.isClass() && !NON_OCCURRENCE_TERMS.contains(t) && t != GbifTerm.gbifID;
        }
      })
      );
  }

  /**
   * Lists all terms relevant for a verbatim occurrence record.
   * occurrenceID is included and comes first, but its the verbatim term, not the GBIF key, so be careful.
   * UnknownTerms are not included as they are open ended.
   */
  public static Iterable<? extends Term> verbatimTerms() {
    return Iterables.concat(
      Lists.newArrayList(GbifTerm.gbifID),
      Iterables.filter(Lists.newArrayList(DcTerm.values()), new Predicate<DcTerm>() {

        @Override
        public boolean apply(@Nullable DcTerm t) {
          return !t.isClass();
        }
      }), Iterables.filter(Lists.newArrayList(DwcTerm.values()), new Predicate<DwcTerm>() {

        @Override
        public boolean apply(@Nullable DwcTerm t) {
          return !t.isClass() && !NON_OCCURRENCE_TERMS.contains(t);
        }
      })
      );
  }

  /**
   * @return true if the term is an interpreted date and stored as a binary in HBase
   */
  public static boolean isInterpretedDate(Term term) {
    return INTERPRETED_DATES.contains(term);
  }

  /**
   * @return true if the term is an interpreted numerical and stored as a binary in HBase
   */
  public static boolean isInterpretedNumerical(Term term) {
    return INTERPRETED_NUM.contains(term);
  }

  /**
   * @return true if the term is an interpreted double and stored as a binary in HBase
   */
  public static boolean isInterpretedDouble(Term term) {
    return INTERPRETED_DOUBLE.contains(term);
  }

  /**
   * @return true if the term is an interpreted boolean and stored as a binary in HBase
   */
  public static boolean isInterpretedBoolean(Term term) {
    return INTERPRETED_BOOLEAN.contains(term);
  }

}
