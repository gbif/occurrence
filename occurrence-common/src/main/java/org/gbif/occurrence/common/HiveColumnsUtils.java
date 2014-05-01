package org.gbif.occurrence.common;

import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import com.google.common.collect.ImmutableSet;

/**
 * Utility class to handle column names in Hive for Terms, OccurrenceIssues and Extensions.
 */
public class HiveColumnsUtils {

  // reserved hive words
  public static final ImmutableSet<String> HIVE_RESERVED_WORDS = new ImmutableSet.Builder<String>().add("date",
    "order", "format", "group").build();

  // prefix for extension columns
  private static final String EXTENSION_PRE = "ext_";

  private HiveColumnsUtils() {
    // empty constructor
  }

  /**
   * Gets the Hive column name of the term parameter.
   */
  public static String getHiveColumn(Term term) {
    final String columnName = term.simpleName().toLowerCase();
    if (HIVE_RESERVED_WORDS.contains(columnName)) {
      return columnName + '_';
    }
    return columnName;
  }

  /**
   * Gets the Hive column name of the extension parameter.
   */
  public static String getHiveColumn(Extension extension) {
    return EXTENSION_PRE + extension.name().toLowerCase();
  }


  /**
   * Returns the Hive data type of term parameter.
   */
  public static String getHiveType(Term term) {
    if (TermUtils.isInterpretedNumerical(term)) {
      return "INT";
    } else if (TermUtils.isInterpretedDate(term)) {
      return "BIGINT";
    } else if (TermUtils.isInterpretedDouble(term)) {
      return "DOUBLE";
    } else if (TermUtils.isInterpretedBoolean(term)) {
      return "BOOLEAN";
    } else if (GbifTerm.mediaType == term || GbifTerm.issue == term) {
      return "ARRAY<STRING>";
    } else {
      return "STRING";
    }
  }

  /**
   * Gets the Hive column name of the occurrence issue parameter.
   */
  public static String getHiveColumn(OccurrenceIssue issue) {
    final String columnName = issue.name().toLowerCase();
    if (HIVE_RESERVED_WORDS.contains(columnName)) {
      return columnName + '_';
    }
    return columnName;
  }
}
