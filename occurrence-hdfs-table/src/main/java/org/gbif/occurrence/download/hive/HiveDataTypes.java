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

import static org.gbif.occurrence.common.TermUtils.isVocabulary;

import com.google.common.collect.ImmutableSet;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.EcoTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;

/**
 * Utilities to provide the Hive data type for a given term.
 *
 * <p>This effectively encapsulated to the logic to provide the Hive data type for any term, which
 * may vary depending on whether it is used in the verbatim or interpreted context. E.g.
 * dwc:decimalLatitude may be a hive STRING when used in verbatim, but a DOUBLE when interpreted.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class HiveDataTypes {

  public static final String TYPE_STRING = "STRING";
  public static final String TYPE_BOOLEAN = "BOOLEAN";
  public static final String TYPE_INT = "INT";
  public static final String TYPE_DOUBLE = "DOUBLE";
  public static final String TYPE_BIGINT = "BIGINT";
  public static final String TYPE_ARRAY_STRING = "ARRAY<STRING>";
  public static final String TYPE_VOCABULARY_STRUCT =
      "STRUCT<concept: STRING,lineage: ARRAY<STRING>>";
  public static final String TYPE_VOCABULARY_ARRAY_STRUCT =
      "STRUCT<concepts: ARRAY<STRING>,lineage: ARRAY<STRING>>";
  public static final String TYPE_MAP_STRUCT = "MAP<STRING, STRING>";
  public static final String TYPE_MAP_OF_ARRAY_STRUCT = "MAP<STRING, ARRAY<STRING>>";
  public static final String TYPE_MAP_OF_MAP_STRUCT = "MAP<STRING, MAP<STRING, STRING>>";
  public static final String TYPE_MAP_OF_MAP_ARRAY_STRUCT = "MAP<STRING, MAP<STRING, ARRAY<STRING>>>";
  public static final String TYPE_ARRAY_PARENT_STRUCT =
      "ARRAY<STRUCT<id: STRING,eventType: STRING>>";
  public static final String GEOLOGICAL_RANGE_STRUCT = "STRUCT<gt: DOUBLE,lte: DOUBLE>";
  public static final String TYPE_TIMESTAMP = "TIMESTAMP";
  // An index of types for terms, if used in the interpreted context
  public static final Map<Term, String> TYPED_TERMS;
  private static final Set<Term> ARRAY_STRING_TERMS =
      ImmutableSet.of(
          GbifTerm.mediaType,
          GbifTerm.issue,
          GbifInternalTerm.networkKey,
          DwcTerm.recordedByID,
          DwcTerm.identifiedByID,
          GbifInternalTerm.dwcaExtension,
          DwcTerm.typeStatus,
          DwcTerm.datasetID,
          DwcTerm.datasetName,
          DwcTerm.recordedBy,
          DwcTerm.identifiedBy,
          DwcTerm.otherCatalogNumbers,
          DwcTerm.preparations,
          DwcTerm.samplingProtocol,
          GbifInternalTerm.parentEventGbifId,
          GbifTerm.projectId,
          DwcTerm.higherGeography,
          DwcTerm.georeferencedBy,
          DwcTerm.associatedSequences,
          GbifTerm.lithostratigraphy,
          GbifTerm.biostratigraphy,
          GbifTerm.dnaSequenceID,
          GbifTerm.checklistKey,
          EcoTerm.verbatimSiteNames,
          EcoTerm.verbatimSiteDescriptions,
          EcoTerm.targetHabitatScope,
          EcoTerm.excludedHabitatScope,
          EcoTerm.taxonCompletenessProtocols,
          EcoTerm.targetGrowthFormScope,
          EcoTerm.excludedGrowthFormScope,
          EcoTerm.compilationTypes,
          EcoTerm.compilationSourceTypes,
          EcoTerm.inventoryTypes,
          EcoTerm.protocolNames,
          EcoTerm.protocolDescriptions,
          EcoTerm.protocolReferences,
          EcoTerm.voucherInstitutions,
          EcoTerm.materialSampleTypes,
          EcoTerm.samplingPerformedBy);

  // dates are all stored as BigInt
  private static final Set<Term> BIGINT_TERMS =
      Set.of(
          DwcTerm.dateIdentified,
          GbifTerm.lastInterpreted,
          GbifTerm.lastParsed,
          GbifTerm.lastCrawled,
          DcTerm.modified,
          GbifInternalTerm.fragmentCreated,
          GbifInternalTerm.eventDateGte,
          GbifInternalTerm.eventDateLte);

  private static final Set<Term> INT_TERMS =
      Set.of(
          DwcTerm.year,
          DwcTerm.month,
          DwcTerm.day,
          DwcTerm.startDayOfYear,
          DwcTerm.endDayOfYear,
          GbifInternalTerm.crawlId,
          GbifInternalTerm.identifierCount,
          DwcTerm.individualCount,
          EcoTerm.siteCount,
          EcoTerm.abundanceCap);

  private static final Set<Term> DOUBLE_TERMS =
      Set.of(
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
          GbifTerm.relativeOrganismQuantity,
          GbifTerm.distanceFromCentroidInMeters,
          GbifInternalTerm.humboldtEventDurationValueInMinutes,
          EcoTerm.samplingEffortValue,
          EcoTerm.geospatialScopeAreaValue,
          EcoTerm.totalAreaSampledValue,
          EcoTerm.eventDurationValue);

  private static final Set<Term> BOOLEAN_TERMS =
      Set.of(
          GbifTerm.hasCoordinate,
          GbifTerm.hasGeospatialIssues,
          GbifTerm.repatriated,
          GbifInternalTerm.isInCluster,
          GbifTerm.isSequenced,
          EcoTerm.isTaxonomicScopeFullyReported,
          EcoTerm.isAbsenceReported,
          EcoTerm.hasNonTargetTaxa,
          EcoTerm.areNonTargetTaxaFullyReported,
          EcoTerm.isLifeStageScopeFullyReported,
          EcoTerm.isDegreeOfEstablishmentScopeFullyReported,
          EcoTerm.isGrowthFormScopeFullyReported,
          EcoTerm.hasNonTargetOrganisms,
          EcoTerm.isAbundanceReported,
          EcoTerm.isAbundanceCapReported,
          EcoTerm.isVegetationCoverReported,
          EcoTerm.isLeastSpecificTargetCategoryQuantityInclusive,
          EcoTerm.hasVouchers,
          EcoTerm.hasMaterialSamples,
          EcoTerm.isSamplingEffortReported);

  private static final Set<Term> TYPE_MAP_OF_MAP_ARRAY_STRUCT_TERMS =
      Set.of(
          EcoTerm.targetTaxonomicScope,
          EcoTerm.excludedTaxonomicScope,
          EcoTerm.absentTaxa,
          EcoTerm.nonTargetTaxa);

  static {
    // build the term type index of Term -> Type
    TYPED_TERMS =
        Stream.of(
                INT_TERMS.stream().map(t -> new AbstractMap.SimpleEntry<>(t, TYPE_INT)),
                BIGINT_TERMS.stream().map(t -> new AbstractMap.SimpleEntry<>(t, TYPE_BIGINT)),
                DOUBLE_TERMS.stream().map(t -> new AbstractMap.SimpleEntry<>(t, TYPE_DOUBLE)),
                BOOLEAN_TERMS.stream().map(t -> new AbstractMap.SimpleEntry<>(t, TYPE_BOOLEAN)),
                TYPE_MAP_OF_MAP_ARRAY_STRUCT_TERMS.stream()
                    .map(t -> new AbstractMap.SimpleEntry<>(t, TYPE_MAP_OF_MAP_ARRAY_STRUCT)),
                ARRAY_STRING_TERMS.stream()
                    .map(t -> new AbstractMap.SimpleEntry<>(t, TYPE_ARRAY_STRING)))
            .flatMap(Function.identity())
            .collect(
                Collectors.collectingAndThen(
                    Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue),
                    Collections::unmodifiableMap));
  }

  /**
   * Provides the Hive data type to use for the given term and context (e.g. verbatim or
   * interpreted) that it is being used.
   *
   * @param term to retrieve the type for
   * @param verbatimContext true if the term is being used in the verbatim context, otherwise false
   * @return The correct hive type for the term in the context in which it is being used
   */
  public static String typeForTerm(Term term, boolean verbatimContext) {
    // regardless of context, the GBIF ID is always typed
    if (GbifTerm.gbifID == term) {
      return TYPE_STRING;
    } else if (verbatimContext) {
      return TYPE_STRING; // verbatim are always string
    } else if (GbifInternalTerm.parentEventGbifId == term) {
      return TYPE_ARRAY_PARENT_STRUCT;
    } else if (GbifTerm.geologicalTime == term) {
      return GEOLOGICAL_RANGE_STRUCT;
    } else if (isVocabulary(term)) {
      if (TermUtils.isArray(term)) {
        return TYPE_VOCABULARY_ARRAY_STRUCT;
      } else {
        return TYPE_VOCABULARY_STRUCT;
      }
    } else if (term.equals(GbifInternalTerm.classifications)) {
      return TYPE_MAP_OF_ARRAY_STRUCT;
    } else if (term.equals(GbifInternalTerm.taxonomicStatuses)) {
      return TYPE_MAP_STRUCT;
    } else if (term.equals(GbifTerm.nonTaxonomicIssue)) {
      return TYPE_ARRAY_STRING;
    } else if (term.equals(GbifTerm.taxonomicIssue)) {
      return TYPE_MAP_OF_ARRAY_STRUCT;
    } else if (term.equals(GbifInternalTerm.classificationDetails)) {
      return TYPE_MAP_OF_MAP_STRUCT;
    } else {
      return TYPED_TERMS.getOrDefault(term, TYPE_STRING); // interpreted term with a registered type
    }
  }
}
