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

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.terms.utils.TermUtils;

import java.util.Locale;
import java.util.Map;

/**
 * Utilities related to the actual queries executed at runtime — these functions for generating Parquet
 * downloads.
 */
class ParquetQueries extends Queries {

  @Override
  String toHiveDataType(Term term) {
    return HiveDataTypes.typeForTerm(term, false);
  }

  @Override
  String toVerbatimHiveInitializer(Term term) {
    return HiveColumns.getVerbatimColPrefix() + term.simpleName().toLowerCase(Locale.UK);
  }

  @Override
  String toHiveInitializer(Term term) {
    return term.simpleName().toLowerCase();
  }

  @Override
  String toInterpretedHiveInitializer(Term term, String checklistKey, String denormalisedTaxonomy,
                                      Map<String, String> checklistNestedStructMap) {
    if (TermUtils.isTaxonomic(term)) {
      return toTaxonomicHiveInitializer(term, checklistKey, denormalisedTaxonomy, checklistNestedStructMap);
    } else if (TermUtils.isInterpretedLocalDateSeconds(term)
        || TermUtils.isInterpretedUtcDateSeconds(term)
        || TermUtils.isInterpretedUtcDateMilliseconds(term)) {
      return "cast(from_unixtime(" + HiveColumns.columnFor(term) + ") as timestamp)";
    } else if (TermUtils.isVocabulary(term)) {
      if (TermUtils.isArray(term)) {
        return toNestedHiveInitializer(term, "concepts");
      } else {
        return toNestedHiveInitializer(term, "concept");
      }
    } else {
      return term.simpleName().toLowerCase();
    }
  }

  /**
   *
   * @param term the term to select
   * @param denormalisedTaxonomy the UUID of the taxonomy in the top level fields (e.g. COL)
   * @param checklistKey the checklist to use in the SELECT
   * @param checklistNestedStructMap map of checklist UUID to nested struct name e.g. `gbif_classification`
   * @return
   */
   protected static String toTaxonomicHiveInitializer(Term term,
                                           String checklistKey,
                                           String denormalisedTaxonomy,
                                           Map<String, String> checklistNestedStructMap) {
    if (checklistKey == null || checklistKey.isEmpty()) {
      throw new IllegalArgumentException("checklistKey must not be null or empty");
    }

    if (denormalisedTaxonomy == null || denormalisedTaxonomy.isEmpty()) {
      throw new IllegalArgumentException("denormalisedTaxonomy must not be null or empty");
    }

    if (!checklistKey.equals(denormalisedTaxonomy) && !checklistNestedStructMap.containsKey(checklistKey)) {
      // If the checklist key is not the denormalised taxonomy, but is in the nested struct map, use it
      throw new IllegalArgumentException("checklistKey is not supported for downloads ! Check configuration" +
        " for the checklistNestedStructMap and denormalisedTaxonomy properties");
    }

    String prefix = "";
    if (!checklistKey.equals(denormalisedTaxonomy)) {
      prefix = "occurrence." + checklistNestedStructMap.get(checklistKey) + ".";
    }

    if (term == GbifTerm.issue) {
      // combine the non taxonomic issues with the
      // taxonomic issues from the specified checklist
      return String.format(
        "array_union(nontaxonomicissue, %s) as issue",
        prefix + "taxonomicissue");
    } else if (term == DwcTerm.infragenericEpithet) {
      //FIX ME
      // gets around the fact that infragenericEpithet is present in the denormalised taxonomy,
      // but is NOT present in the nested struct
      return String.format("NULL AS %s", HiveColumns.columnFor(term));
    } else {
      final String columnName = HiveColumns.columnFor(term);
      return String.format(
        "%s%s AS %s",
        prefix,
        columnName,
        columnName
      );
    }
  }
}
