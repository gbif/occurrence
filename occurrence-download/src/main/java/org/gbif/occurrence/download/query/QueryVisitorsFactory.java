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
package org.gbif.occurrence.download.query;

import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.predicate.query.SQLQueryVisitor;
import org.gbif.predicate.query.occurrence.OccurrenceTermsMapper;

import java.util.Map;

public class QueryVisitorsFactory {

  /**
   * Create a SQL Query visitor
   *
   * @param denormalisedTaxonomy the UUID of the taxonomy in the top level fields (e.g. COL)
   * @param checklistNestedStructMap map of checklist UUID to nested struct name e.g. `gbif_classification`
   * @param defaultChecklistKey the default checklist to use for query construction
   * @param disambiguationTable the table to use when we have 2 columns of the same name
   * @return a SQLQueryVisitor instance
   */
  public static SQLQueryVisitor<SearchParameter> createSqlQueryVisitor(
      String denormalisedTaxonomy,
      Map<String, String> checklistNestedStructMap,
      String defaultChecklistKey,
      String disambiguationTable) {

    return new SQLQueryVisitor<>(
        new OccurrenceTermsMapper(),
        denormalisedTaxonomy,
        checklistNestedStructMap,
        defaultChecklistKey,
        disambiguationTable
    );
  }
}
