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

import java.util.Locale;

/**
 * Utilities related to the actual queries executed at runtime — these functions for generating downloads.
 */
abstract class TsvQueries extends Queries {

  @Override
  String toVerbatimHiveInitializer(Term term) {
    return HiveColumns.getVerbatimColPrefix() + term.simpleName().toLowerCase(Locale.UK);
  }

  @Override
  String toHiveInitializer(Term term) {
    return HiveColumns.columnFor(term);
  }

  /**
   * Transforms the term into toISO8601(hiveColumn) expression.
   */
  protected static String toISO8601Initializer(Term term) {
    final String column = HiveColumns.columnFor(term);
    return "toISO8601(" + column + ") AS " + column;
  }

  /**
   * Transforms the term into toLocalISO8601(hiveColumn) expression.
   */
  protected static String toLocalISO8601Initializer(Term term) {
    final String column = HiveColumns.columnFor(term);
    return "toLocalISO8601(" + column + ") AS " + column;
  }
}
