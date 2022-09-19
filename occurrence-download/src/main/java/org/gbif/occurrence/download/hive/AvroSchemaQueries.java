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

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;

/**
 * Utilities related to the actual queries executed at runtime — these functions for generating the AVRO download schemas.
 */
class AvroSchemaQueries extends Queries {

  @Override
  String toHiveDataType(Term term) {
    if (TermUtils.isInterpretedUtcDate(term) ||
      TermUtils.isInterpretedLocalDate(term) ||
      TermUtils.isVocabulary(term)) {
      return HiveDataTypes.TYPE_STRING;
    } else {
      return HiveDataTypes.typeForTerm(term, false);
    }
  }

  @Override
  String toVerbatimHiveInitializer(Term term) {
    return HiveColumns.getVerbatimColPrefix() + term.simpleName();
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
