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

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;

import java.util.Locale;

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
  String toInterpretedHiveInitializer(Term term) {
    if (TermUtils.isInterpretedLocalDateSeconds(term)
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
}
