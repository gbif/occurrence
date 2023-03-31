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
package org.gbif.occurrence.download.predicate;

import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.occurrence.download.query.QueryVisitorsFactory;
import org.gbif.occurrence.search.es.OccurrenceBaseEsFieldMapper;

import java.util.Optional;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;

@UtilityClass
public class EsPredicateUtil {

  private static final ObjectMapper OBJECT_MAPPER =
    new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  static {
    OBJECT_MAPPER.addMixIn(SearchParameter.class, QueryVisitorsFactory.OccurrenceSearchParameterMixin.class);
  }

  @SneakyThrows
  private Predicate toPredicateApi(org.gbif.api.model.occurrence.predicate.Predicate predicate) {
    return OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(predicate), Predicate.class);
  }

  @SneakyThrows
  public static QueryBuilder searchQuery(Predicate predicate, OccurrenceBaseEsFieldMapper esFieldMapper) {
    Optional<QueryBuilder> queryBuilder = QueryVisitorsFactory.createEsQueryVisitor(esFieldMapper)
      .getQueryBuilder(predicate);
    if (queryBuilder.isPresent()) {
      BoolQueryBuilder query = (BoolQueryBuilder) queryBuilder.get();
      esFieldMapper.getDefaultFilter().ifPresent(df -> query.filter().add(df));
      return query;
    }
    return esFieldMapper.getDefaultFilter().orElse(QueryBuilders.matchAllQuery());
  }

  public static QueryBuilder searchQuery(org.gbif.api.model.occurrence.predicate.Predicate predicate, OccurrenceBaseEsFieldMapper esFieldMapper) {
    return searchQuery(toPredicateApi(predicate), esFieldMapper);
  }
}
