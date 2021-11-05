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
import org.gbif.occurrence.common.HiveColumnsUtils;
import org.gbif.occurrence.common.TermUtils;

/**
 * Utilities related to the actual queries executed at runtime — these functions for generating TSV downloads.
 */
public class HiveQueries extends TsvQueries {

  @Override
  String toHiveDataType(Term term) {
    return HiveDataTypes.TYPE_STRING;
  }

  @Override
  String toInterpretedHiveInitializer(Term term) {
    if (TermUtils.isInterpretedLocalDate(term)) {
      return toLocalISO8601Initializer(term);
    } else if (TermUtils.isInterpretedUtcDate(term)) {
      return toISO8601Initializer(term);
    } else if (HiveColumnsUtils.isHiveArray(term)) {
      return String.format(toArrayInitializer(term), HiveColumns.columnFor(term));
    } else {
      return HiveColumns.columnFor(term);
    }
  }

  /**
   * Transforms the term into a joinArray(…) expression.
   */
  protected static String toArrayInitializer(Term term) {
    return String.format("if(%1$s IS NULL,'',joinArray(%1$s,'\\\\;')) AS %1$s", HiveColumns.columnFor(term));
  }

}
