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
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.occurrence.search.es.OccurrenceBaseEsFieldMapper;
import org.gbif.occurrence.search.predicate.EsQueryVisitorFactory;
import org.gbif.predicate.query.EsQueryVisitor;
import org.gbif.predicate.query.SQLQueryVisitor;
import org.gbif.predicate.query.occurrence.OccurrenceTermsMapper;

public class QueryVisitorsFactory {

  public static SQLQueryVisitor<SearchParameter> createSqlQueryVisitor() {
    return new SQLQueryVisitor<>(new OccurrenceTermsMapper());
  }

  public static SQLQueryVisitor<SearchParameter> createSqlQueryVisitor(String defaultChecklistKey) {
    return new SQLQueryVisitor<>(new OccurrenceTermsMapper(), defaultChecklistKey);
  }

  public static EsQueryVisitor<OccurrenceSearchParameter> createEsQueryVisitor(
      OccurrenceBaseEsFieldMapper fieldMapper) {
    return EsQueryVisitorFactory.createEsQueryVisitor(fieldMapper);
  }
}
