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

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;


/**
 * Utilities related to the actual queries executed at runtime — these functions for generating AVRO downloads.
 */
class ParquetSchemaQueries extends Queries {

  @Override
  String toHiveDataType(Term term) {
    if (DwcTerm.establishmentMeans.equals(term)) {
      // Just using the main establishmentMeans, not the whole lineage.
      // (Inform Google BigQuery before changing.)
      return HiveDataTypes.TYPE_STRING;
    } else if (TermUtils.isInterpretedLocalDate(term)) {
      return HiveDataTypes.TYPE_TIMESTAMP;
    } else if (TermUtils.isInterpretedUtcDate(term)) {
      return HiveDataTypes.TYPE_TIMESTAMP;
    } else if (TermUtils.isVocabulary(term)) {
      return HiveDataTypes.TYPE_ARRAY_STRING;
    } else {
      return HiveDataTypes.typeForTerm(term, false);
    }
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
