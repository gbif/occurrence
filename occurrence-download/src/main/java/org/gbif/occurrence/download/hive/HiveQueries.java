package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.HiveColumnsUtils;
import org.gbif.occurrence.common.TermUtils;

/**
 * Utilities related to the actual queries executed at runtime — these functions for generating TSV downloads.
 */
class HiveQueries extends TsvQueries {

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

  HiveQueries() {}
}
