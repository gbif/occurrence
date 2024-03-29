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

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;

import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EsFulltextSuggestBuilder {

  private final OccurrenceBaseEsFieldMapper occurrenceBaseEsFieldMapper;

  private static boolean isPhraseQuery(String query) {
    return query.contains(" ");
  }


  private BoolQueryBuilder buildSuggestQuery(EsField esField, String query) {
    BoolQueryBuilder suggestQuery = new BoolQueryBuilder();

    suggestQuery.should()
      .add(QueryBuilders.prefixQuery(esField.getExactMatchFieldName(), query));

    suggestQuery.should()
      .add(isPhraseQuery(query)? QueryBuilders.matchPhraseQuery(esField.getSearchFieldName(), query) : QueryBuilders.matchQuery(
        esField.getSearchFieldName(), query));

    suggestQuery.minimumShouldMatch(1);

    occurrenceBaseEsFieldMapper.getDefaultFilter().ifPresent(dq -> suggestQuery.filter().add(dq));

    return suggestQuery;
  }

  SearchSourceBuilder buildSuggestFullTextQuery(String query, OccurrenceSearchParameter parameter, Integer limit) {
    EsField esField = occurrenceBaseEsFieldMapper.getEsField(parameter);

    return new SearchSourceBuilder()
      .size(0)
      .fetchSource(false)
      .query(buildSuggestQuery(occurrenceBaseEsFieldMapper.getEsField(parameter), query))
      .aggregation(AggregationBuilders.terms(esField.getSearchFieldName()).field(esField.getExactMatchFieldName()).size(limit));

  }

  List<String> buildSuggestFullTextResponse(OccurrenceSearchParameter occurrenceSearchParameter, SearchResponse response) {
    return
    ((Terms)response.getAggregations().get(occurrenceBaseEsFieldMapper.getSearchFieldName(occurrenceSearchParameter)))
      .getBuckets().stream()
      .map(Terms.Bucket::getKeyAsString)
      .collect(Collectors.toList());
  }

}
