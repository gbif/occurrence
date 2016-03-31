package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

/**
 * Utilities to provide the Hive data type for a given term.
 * <p/>
 * This effectively encapsulated to the logic to provide the Hive data type for any term, which may vary depending on
 * whether it is used in the verbatim or interpreted context.  E.g. dwc:decimalLatitude may be a hive STRING when used
 * in verbatim, but a DOUBLE when interpreted.
 */
final class HiveDataTypes {

  static final String TYPE_STRING = "STRING";
  static final String TYPE_BOOLEAN = "BOOLEAN";
  static final String TYPE_INT = "INT";
  static final String TYPE_DOUBLE = "DOUBLE";
  static final String TYPE_BIGINT = "BIGINT";
  static final String TYPE_ARRAY_STRING = "ARRAY<STRING>";
  static final String TYPE_DECIMAL = "DECIMAL";
  // An index of types for terms, if used in the interpreted context
  private static final Map<Term, String> TYPED_TERMS;
  private static final Set<Term> ARRAY_STRING_TERMS = ImmutableSet.<Term>of(GbifTerm.mediaType, GbifTerm.issue);
  private static final Set<Term> BIGINT_TERMS = ImmutableSet.<Term>of(
    // dates are all stored as BigInt
    DwcTerm.eventDate,
    DwcTerm.dateIdentified,
    GbifTerm.lastInterpreted,
    GbifTerm.lastParsed,
    GbifTerm.lastCrawled,
    DcTerm.modified,
    GbifInternalTerm.fragmentCreated);
  private static final Set<Term> INT_TERMS = ImmutableSet.<Term>of(GbifTerm.gbifID,
                                                                   DwcTerm.year,
                                                                   DwcTerm.month,
                                                                   DwcTerm.day,
                                                                   GbifTerm.taxonKey,
                                                                   GbifTerm.kingdomKey,
                                                                   GbifTerm.phylumKey,
                                                                   GbifTerm.classKey,
                                                                   GbifTerm.orderKey,
                                                                   GbifTerm.familyKey,
                                                                   GbifTerm.genusKey,
                                                                   GbifTerm.subgenusKey,
                                                                   GbifTerm.speciesKey,
                                                                   GbifInternalTerm.crawlId,
                                                                   GbifInternalTerm.identifierCount);
  private static final Set<Term> DOUBLE_TERMS = ImmutableSet.<Term>of(DwcTerm.decimalLatitude,
                                                                      DwcTerm.decimalLongitude,
                                                                      GbifTerm.coordinateAccuracy,
                                                                      GbifTerm.elevation,
                                                                      GbifTerm.elevationAccuracy,
                                                                      GbifTerm.depth,
                                                                      GbifTerm.depthAccuracy);
  private static final Set<Term> BOOLEAN_TERMS =
    ImmutableSet.<Term>of(GbifTerm.hasCoordinate, GbifTerm.hasGeospatialIssues);
  private static final Set<Term> DECIMAL_TERMS = ImmutableSet.<Term>of(DwcTerm.coordinateUncertaintyInMeters,
          DwcTerm.coordinatePrecision);
  static {
    // build the term type index of Term -> Type
    TYPED_TERMS = ImmutableMap.<Term, String>builder()
      .putAll(Maps.asMap(INT_TERMS, Functions.constant(TYPE_INT)))
      .putAll(Maps.asMap(BIGINT_TERMS, Functions.constant(TYPE_BIGINT)))
      .putAll(Maps.asMap(DOUBLE_TERMS, Functions.constant(TYPE_DOUBLE)))
      .putAll(Maps.asMap(BOOLEAN_TERMS, Functions.constant(TYPE_BOOLEAN)))
      .putAll(Maps.asMap(ARRAY_STRING_TERMS, Functions.constant(TYPE_ARRAY_STRING)))
      .putAll(Maps.asMap(DECIMAL_TERMS, Functions.constant(TYPE_DECIMAL)))
            .build();
  }

  /**
   * Provides the Hive data type to use for the given term and context (e.g. verbatim or interpreted) that it is being
   * used.
   *
   * @param term            to retrive the type for
   * @param verbatimContext true if the term is being used in the verbatim context, otherwise false
   *
   * @return The correct hive type for the term in the context in which it is being used
   */
  public static String typeForTerm(Term term, boolean verbatimContext) {
    // regardless of context, the GBIF ID is always typed
    if (GbifTerm.gbifID == term) {
      return TYPED_TERMS.get(GbifTerm.gbifID);

    } else if (verbatimContext) {
      return TYPE_STRING; // verbatim are always string

    } else {
      return TYPED_TERMS.containsKey(term)
        ? TYPED_TERMS.get(term)
        : TYPE_STRING; // interpreted term with a registered type

    }
  }

  /**
   * Hidden constructor.
   */
  private HiveDataTypes() {
    //empty default constructor.
  }
}
