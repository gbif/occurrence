package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.Term;

/**
 * Utilities related to the actual queries executed at runtime — these functions for generating the AVRO download schemas.
 */
class AvroSchemaQueries extends Queries {

  @Override
  String toHiveDataType(Term term) {
    return HiveDataTypes.typeForTerm(term, false);
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
