package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.Term;

import java.util.Locale;

/**
 * Utilities related to the actual queries executed at runtime — these functions for generating downloads.
 */
abstract class TsvQueries extends Queries {

  @Override
  String toVerbatimHiveInitializer(Term term) {
    return HiveColumns.VERBATIM_COL_PREFIX + term.simpleName().toLowerCase(Locale.UK);
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
