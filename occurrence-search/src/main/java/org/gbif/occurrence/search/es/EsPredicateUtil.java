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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.occurrence.common.json.OccurrenceSearchParameterMixin;
import org.gbif.predicate.query.OccurrenceEsQueryVisitor;
import org.gbif.search.es.occurrence.OccurrenceEsFieldMapper;

@UtilityClass
public class EsPredicateUtil {

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  static {
    OBJECT_MAPPER.addMixIn(SearchParameter.class, OccurrenceSearchParameterMixin.class);
  }

  @SneakyThrows
  public static QueryBuilder searchQuery(
      Predicate predicate, OccurrenceEsFieldMapper esFieldMapper) {
    Optional<QueryBuilder> queryBuilder =
        new OccurrenceEsQueryVisitor(esFieldMapper, "defaultChecklistKey")
            .getQueryBuilder(predicate);
    if (queryBuilder.isPresent()) {
      BoolQueryBuilder query = (BoolQueryBuilder) queryBuilder.get();
      esFieldMapper.getDefaultFilter().ifPresent(df -> query.filter().add(df));
      return query;
    }
    return esFieldMapper.getDefaultFilter().orElse(QueryBuilders.matchAllQuery());
  }
}
