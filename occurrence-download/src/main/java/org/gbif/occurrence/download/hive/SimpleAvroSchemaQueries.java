package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;

/**
 * Utilities related to the actual queries executed at runtime — these functions for generating the AVRO download schemas.
 *
 * This subclass uses "verbatim" as a field name prefix, rather than "v_".
 */
class SimpleAvroSchemaQueries extends AvroSchemaQueries {

  @Override
  String toVerbatimHiveInitializer(Term term) {
    if (term.simpleName().startsWith("verbatim")) {
      return term.simpleName();
    } else {
      return "verbatim" + Character.toUpperCase(term.simpleName().charAt(0)) + term.simpleName().substring(1);
    }
  }
}
