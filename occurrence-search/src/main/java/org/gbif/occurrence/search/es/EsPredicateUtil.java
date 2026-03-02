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
package org.gbif.occurrence.search.es;

import java.util.Optional;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import lombok.experimental.UtilityClass;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.predicate.query.OccurrenceEsQueryVisitor;
import org.gbif.search.es.occurrence.OccurrenceEsFieldMapper;

@UtilityClass
public class EsPredicateUtil {

  public static Query searchQuery(
      Predicate predicate, OccurrenceEsFieldMapper esFieldMapper, String defaultChecklistKey) {
    Optional<Query> queryBuilder;
    try {
      queryBuilder =
          new OccurrenceEsQueryVisitor(esFieldMapper, defaultChecklistKey).getQueryBuilder(predicate);
    } catch (Exception e) {
      throw new IllegalArgumentException("Error building predicate query", e);
    }
    if (queryBuilder.isPresent()) {
      return
          esFieldMapper
              .getDefaultFilter()
              .map(df -> Query.of(q -> q.bool(b -> b.must(queryBuilder.get()).filter(df))))
              .orElse(queryBuilder.get());
    }
    return esFieldMapper
        .getDefaultFilter()
        .orElseGet(() -> Query.of(q -> q.matchAll(m -> m)));
  }
}
