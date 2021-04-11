package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;

import java.util.Locale;

/**
 * Utilities related to the actual queries executed at runtime — these functions for generating AVRO downloads.
 */
class ParquetSchemaQueries extends Queries {

  @Override
  String toHiveDataType(Term term) {
    return HiveDataTypes.typeForTerm(term, false);
  }

  @Override
  String toVerbatimHiveInitializer(Term term) {
    if (term.simpleName().startsWith("verbatim")) {
      return term.simpleName();
    } else {
      return "verbatim" + Character.toUpperCase(term.simpleName().charAt(0)) + term.simpleName().substring(1);
    }
  }

  @Override
  String toHiveInitializer(Term term) {
    return term.simpleName();
  }

  @Override
  String toInterpretedHiveInitializer(Term term) {
    return term.simpleName();
  }
}
