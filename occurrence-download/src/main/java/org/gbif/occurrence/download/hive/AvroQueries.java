package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;

/**
 * Utilities related to the actual queries executed at runtime — these functions for generating AVRO downloads.
 */
class AvroQueries extends TsvQueries {

  @Override
  String toHiveDataType(Term term) {
    return HiveDataTypes.typeForTerm(term, false);
  }

  @Override
  String toInterpretedHiveInitializer(Term term) {
    if (TermUtils.isInterpretedDate(term)) {
      return toISO8601Initializer(term);
    } else {
      return HiveColumns.columnFor(term);
    }
  }

  AvroQueries() {}
}
