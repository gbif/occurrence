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
package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;

/**
 * Utilities to provide the Hive data type for a given term.
 * <p/>
 * This effectively encapsulated to the logic to provide the Hive data type for any term, which may vary depending on
 * whether it is used in the verbatim or interpreted context.  E.g. dwc:decimalLatitude may be a hive STRING when used
 * in verbatim, but a DOUBLE when interpreted.
 */
public final class HiveDataTypes {

  public static final String TYPE_STRING = "STRING";
  public static final String TYPE_BOOLEAN = "BOOLEAN";
  public static final String TYPE_INT = "INT";
  public static final String TYPE_DOUBLE = "DOUBLE";
  public static final String TYPE_BIGINT = "BIGINT";
  public static final String TYPE_ARRAY_STRING = "ARRAY<STRING>";
  // An index of types for terms, if used in the interpreted context
  private static final Map<Term, String> TYPED_TERMS;
  private static final Set<Term> ARRAY_STRING_TERMS =
    ImmutableSet.of(
      GbifTerm.mediaType,
      GbifTerm.issue,
      GbifInternalTerm.networkKey,
      DwcTerm.recordedByID,
      DwcTerm.identifiedByID,
      GbifInternalTerm.dwcaExtension,
      GbifInternalTerm.lifeStageLineage
    );

  // dates are all stored as BigInt
  private static final Set<Term> BIGINT_TERMS = ImmutableSet.of(
    GbifTerm.gbifID,
    DwcTerm.eventDate,
    DwcTerm.dateIdentified,
    GbifTerm.lastInterpreted,
    GbifTerm.lastParsed,
    GbifTerm.lastCrawled,
    DcTerm.modified,
    GbifInternalTerm.fragmentCreated);

  private static final Set<Term> INT_TERMS = ImmutableSet.of(
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
    GbifTerm.acceptedTaxonKey,
    GbifInternalTerm.crawlId,
    GbifInternalTerm.identifierCount,
    DwcTerm.individualCount);

  private static final Set<Term> DOUBLE_TERMS = ImmutableSet.of(
    DwcTerm.decimalLatitude,
    DwcTerm.decimalLongitude,
    DwcTerm.coordinateUncertaintyInMeters,
    DwcTerm.coordinatePrecision,
    GbifTerm.elevation,
    GbifTerm.elevationAccuracy,
    GbifTerm.depth,
    GbifTerm.depthAccuracy,
    DwcTerm.organismQuantity,
    DwcTerm.sampleSizeValue,
    GbifTerm.relativeOrganismQuantity);

  private static final Set<Term> BOOLEAN_TERMS = ImmutableSet.of(
    GbifTerm.hasCoordinate,
    GbifTerm.hasGeospatialIssues,
    GbifTerm.repatriated,
    GbifInternalTerm.isInCluster);

  static {
    // build the term type index of Term -> Type
    TYPED_TERMS = Stream.of(
      INT_TERMS.stream().map(t -> new AbstractMap.SimpleEntry<>(t, TYPE_INT)),
      BIGINT_TERMS.stream().map(t -> new AbstractMap.SimpleEntry<>(t, TYPE_BIGINT)),
      DOUBLE_TERMS.stream().map(t -> new AbstractMap.SimpleEntry<>(t, TYPE_DOUBLE)),
      BOOLEAN_TERMS.stream().map(t -> new AbstractMap.SimpleEntry<>(t, TYPE_BOOLEAN)),
      ARRAY_STRING_TERMS.stream().map(t -> new AbstractMap.SimpleEntry<>(t, TYPE_ARRAY_STRING))
    ).flatMap(Function.identity())
     .collect(Collectors.collectingAndThen(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue), Collections::unmodifiableMap));
  }

  /**
   * Provides the Hive data type to use for the given term and context (e.g. verbatim or interpreted) that it is being
   * used.
   *
   * @param term            to retrieve the type for
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
      return TYPED_TERMS.getOrDefault(term, TYPE_STRING); // interpreted term with a registered type

    }
  }

  /**
   * Hidden constructor.
   */
  private HiveDataTypes() {
    //empty default constructor.
  }
}
