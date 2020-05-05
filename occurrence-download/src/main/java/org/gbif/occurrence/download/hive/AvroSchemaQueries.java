package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;

/**
 * Utilities related to the actual queries executed at runtime — these functions for generating the AVRO download schemas.
 */
class AvroSchemaQueries extends Queries {

  @Override
  String toHiveDataType(Term term) {
    if (TermUtils.isInterpretedUtcDate(term) || TermUtils.isInterpretedLocalDate(term)) {
      return HiveDataTypes.TYPE_STRING;
    } else {
      return HiveDataTypes.typeForTerm(term, false);
    }
  }

  @Override
  String toVerbatimHiveInitializer(Term term) {
    return HiveColumns.VERBATIM_COL_PREFIX + term.simpleName();
  }

  @Override
  String toInterpretedHiveInitializer(Term term) {
    return term.simpleName();
  }

  @Override
  String toHiveInitializer(Term term) {
    return term.simpleName();
  }

  AvroSchemaQueries() {}
}
