package org.gbif.occurrence.common;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
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

  private static final Set<? extends Term> INTERPRETED_DATES = ImmutableSet.of(
      DwcTerm.eventDate, DwcTerm.dateIdentified);

  private static final Set<? extends Term> INTERPRETED_NUM = ImmutableSet.of(
      DwcTerm.year, DwcTerm.month, DwcTerm.day);

  private static final Set<? extends Term> NON_OCCURRENCE_TERMS = ImmutableSet.copyOf(
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

  private static final Set<? extends Term> INTERPRETED_TERMS = ImmutableSet.of(
    DwcTerm.decimalLatitude, DwcTerm.decimalLongitude,
    DwcTerm.verbatimLatitude, DwcTerm.verbatimLongitude,
    DwcTerm.coordinateUncertaintyInMeters, DwcTerm.coordinatePrecision,
    DwcTerm.continent, DwcTerm.waterBody, DwcTerm.stateProvince, DwcTerm.country, DwcTerm.countryCode,
    DwcTerm.scientificName, DwcTerm.scientificNameAuthorship, DwcTerm.taxonRank,
    DwcTerm.kingdom, DwcTerm.phylum, DwcTerm.class_, DwcTerm.order, DwcTerm.family, DwcTerm.genus, DwcTerm.subgenus,
    DwcTerm.genericName, DwcTerm.specificEpithet, DwcTerm.infraspecificEpithet,
    DcTerm.modified, DwcTerm.dateIdentified, DwcTerm.eventDate, DwcTerm.year, DwcTerm.month, DwcTerm.day,
    DwcTerm.minimumDepthInMeters, DwcTerm.maximumDepthInMeters,
    DwcTerm.minimumElevationInMeters, DwcTerm.maximumElevationInMeters,
    DwcTerm.minimumDistanceAboveSurfaceInMeters, DwcTerm.maximumDistanceAboveSurfaceInMeters
  );

  private TermUtils() {
  }

  /**
   * Lists all terms that can have been used during interpretation and are superseded by an interpreted,
   * typed java Occurrence property.
   * @return iterable of terms that have been used during interpretation
   */
  public static Iterable<? extends Term> interpretedTerms(){
    return INTERPRETED_TERMS;
  }

  public static Iterable<? extends Term> propertyTerms(){
    return Iterables.concat(Iterables.filter(Lists.newArrayList(DcTerm.values()), new Predicate<DcTerm>() {
      @Override
      public boolean apply(@Nullable DcTerm input) {
        return !input.isClass();
      }
    }), Iterables.filter(Lists.newArrayList(DwcTerm.values()), new Predicate<DwcTerm>() {
      @Override
      public boolean apply(@Nullable DwcTerm input) {
        return !input.isClass() && !NON_OCCURRENCE_TERMS.contains(input);
      }
    }), Iterables.filter(Lists.newArrayList(GbifTerm.values()), new Predicate<GbifTerm>() {
      @Override
      public boolean apply(@Nullable GbifTerm input) {
        return !input.isClass() && !NON_OCCURRENCE_TERMS.contains(input);
      }
    }));
  }

  /**
   * @return true if the term is an interpreted date and stored as a binary in HBase
   */
  public static boolean isInterpretedDate(Term term){
    return INTERPRETED_DATES.contains(term);
  }

  /**
   * @return true if the term is an interpreted numerical and stored as a binary in HBase
   */
  public static boolean isInterpretedNumerical(Term term){
    return INTERPRETED_NUM.contains(term);
  }

  /**
   * @return true if the term is interpreted and stored as a binary in HBase
   */
  public static boolean isInterpretedBinary(Term term){
    return INTERPRETED_DATES.contains(term) || INTERPRETED_NUM.contains(term);
  }
}
