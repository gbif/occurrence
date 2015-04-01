package org.gbif.occurrence.download.hive;

import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import com.google.common.collect.ImmutableSet;

/**
 * Utilities related to columns in Hive.
 */
public class HiveColumns {

  // reserved hive words
  private static final ImmutableSet<String> RESERVED_WORDS = ImmutableSet.of(
    "date",
    "order",
    "format",
    "group");

  // prefix for extension columns
  static final String EXTENSION_PREFIX = "ext_";

  // prefix for extension columns
  static final String VERBATIM_COL_PREFIX = "v_";

  private HiveColumns() {
    // empty constructor
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
  private static String escapeColumnName(String columnName) {
    return RESERVED_WORDS.contains(columnName) ? columnName + "_" : columnName;
  }

  /**
   * Gets the Hive column name of the extension parameter.
   */
  public static String columnFor(Extension extension) {
    return escapeColumnName(EXTENSION_PREFIX + extension.name().toLowerCase());
  }

  /**
   * Checks if the term is stored as an Hive array.
   */
  public static boolean isHiveArray(Term term) {
    return GbifTerm.mediaType == term || GbifTerm.issue == term;
  }

  /**
   * Gets the Hive column name of the occurrence issue parameter.
   */
  public static String columnFor(OccurrenceIssue issue) {
    return escapeColumnName(issue.name().toLowerCase());
  }
}
