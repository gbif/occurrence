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

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

/**
 * Utility class to handle column names in Hive for Terms, OccurrenceIssues and Extensions.
 */
public class HiveColumnsUtils {

  // prefix for extension columns
  private static final String EXTENSION_PRE = "ext_";

  private HiveColumnsUtils() {
    // empty constructor
  }

  /**
   * Gets the Hive column name of the extension parameter.
   */
  public static String getHiveQueryColumn(Extension extension) {
    return EXTENSION_PRE + extension.name().toLowerCase();
  }

  /**
   * Returns the Hive data type of term parameter.
   */
  public static String getHiveType(Term term) {
    if (TermUtils.isInterpretedNumerical(term)) {
      return "INT";
    } else if (TermUtils.isInterpretedLocalDateSeconds(term)) {
      return "BIGINT";
    } else if (TermUtils.isInterpretedUtcDateSeconds(term)) {
      return "BIGINT";
    } else if (TermUtils.isInterpretedUtcDateMilliseconds(term)) {
      return "BIGINT";
    } else if (TermUtils.isInterpretedDouble(term)) {
      return "DOUBLE";
    } else if (TermUtils.isInterpretedBoolean(term)) {
      return "BOOLEAN";
    } else if (isHiveArray(term)) {
      return "ARRAY<STRING>";
    } else if(TermUtils.isVocabulary(term)) {
      if (TermUtils.isArray(term)) {
        return "STRUCT<concepts: ARRAY<STRING>,lineage: ARRAY<STRING>>";
      } else {
        return "STRUCT<concept: STRING,lineage: ARRAY<STRING>>";
      }
    } else {
      return "STRING";
    }
  }

  public static boolean isDate(Term term) {
    return TermUtils.isInterpretedLocalDateSeconds(term) || TermUtils.isInterpretedUtcDateSeconds(term) || TermUtils.isInterpretedUtcDateMilliseconds(term);
  }

  /** Checks if the term is stored as a Hive array. */
  public static boolean isHiveArray(Term term) {
    return GbifTerm.mediaType == term
        || GbifTerm.issue == term
        || GbifInternalTerm.networkKey == term
        || DwcTerm.identifiedByID == term
        || DwcTerm.recordedByID == term
        || GbifInternalTerm.dwcaExtension == term
        || DwcTerm.datasetID == term
        || DwcTerm.datasetName == term
        || DwcTerm.otherCatalogNumbers == term
        || DwcTerm.recordedBy == term
        || DwcTerm.identifiedBy == term
        || DwcTerm.preparations == term
        || DwcTerm.samplingProtocol == term
        || DwcTerm.associatedSequences == term
        || DwcTerm.higherGeography == term
        || DwcTerm.georeferencedBy == term
        || GbifTerm.projectId == term;
  }
}
