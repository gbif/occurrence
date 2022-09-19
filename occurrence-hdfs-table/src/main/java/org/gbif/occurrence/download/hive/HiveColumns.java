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

import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.Term;

import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;

import lombok.experimental.UtilityClass;

/**
 * Utilities related to columns in Hive.
 */
@UtilityClass
public final class HiveColumns {

  // prefix for extension columns
  private static final String EXTENSION_PREFIX = "ext_";
  // prefix for extension columns
  private static final String VERBATIM_COL_PREFIX = "v_";
  // reserved hive words
  private static final ImmutableSet<String> RESERVED_WORDS = ImmutableSet.of("date", "order", "format", "group");
  private static final Pattern START_WITH_DIGIT_OR_UNDERSCORE = Pattern.compile("(\\d.*)|(_.*)");

  public static String getVerbatimColPrefix() {
    return VERBATIM_COL_PREFIX;
  }

  /**
   * Gets the Hive column name of the term parameter.
   */
  public static String columnFor(Term term) {
    return escapeColumnName(term.simpleName().toLowerCase());
  }

  /**
   * Escapes the name if required.
   */
  public static String escapeColumnName(String columnName) {
    String newColumnName = RESERVED_WORDS.contains(columnName) ? columnName + '_' : columnName;
    return START_WITH_DIGIT_OR_UNDERSCORE.matcher(newColumnName).matches()? '`' + newColumnName + '`' : newColumnName;
  }

  /**
   * Gets the Hive column name of the extension parameter.
   */
  public static String columnFor(Extension extension) {
    return escapeColumnName(EXTENSION_PREFIX + extension.name().toLowerCase());
  }

  /**
   * Gets the Hive column name of the occurrence issue parameter.
   */
  public static String columnFor(OccurrenceIssue issue) {
    return escapeColumnName(issue.name().toLowerCase());
  }

  private static String hiveColumnName(String columnName) {
    String hiveColumnName = columnName;
    if(columnName.startsWith("_")) {
      hiveColumnName = columnName.substring(1);
    } else if (columnName.startsWith("v__")) {
      hiveColumnName = columnName.substring(0,2) + columnName.substring(3);
    }
    return escapeColumnName(hiveColumnName);
  }

  /**
   * Creates a column expression using the UDF cleanDelimiters(columnFor(term)).
   */
  public static String cleanDelimitersInitializer(Term term) {
    return cleanDelimitersInitializer(columnFor(term));
  }

  /**
   * Creates a column expression using the UDF cleanDelimiters(columnName).
   */
  public static String cleanDelimitersInitializer(String column) {
    return "cleanDelimiters(" + escapeColumnName(column) + ") AS " + hiveColumnName(column);
  }

  /**
   * Creates a column expression using the UDF cleanDelimitersArray(columnName).
   */
  public static String cleanDelimitersArrayInitializer(String column) {
    return "cleanDelimitersArray(" + escapeColumnName(column) + ") AS " + hiveColumnName(column);
  }

  /**
   * Creates a column expression using the UDF cleanDelimitersArray(columnFor(term)).
   */
  public static String cleanDelimitersArrayInitializer(Term term) {
    return cleanDelimitersArrayInitializer(columnFor(term));
  }
}
