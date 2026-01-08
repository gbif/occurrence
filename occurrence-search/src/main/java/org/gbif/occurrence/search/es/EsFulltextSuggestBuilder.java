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

import java.util.List;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.predicate.query.EsField;
import org.gbif.search.es.occurrence.OccurrenceEsFieldMapper;

@Data
@Builder
public class EsFulltextSuggestBuilder {

  private final OccurrenceEsFieldMapper occurrenceEsFieldMapper;

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

    occurrenceEsFieldMapper.getDefaultFilter().ifPresent(dq -> suggestQuery.filter().add(dq));

    return suggestQuery;
  }

  SearchSourceBuilder buildSuggestFullTextQuery(String query, OccurrenceSearchParameter parameter, Integer limit) {
    EsField esField = occurrenceEsFieldMapper.getEsField(parameter);

    return new SearchSourceBuilder()
      .size(0)
      .fetchSource(false)
      .query(buildSuggestQuery(occurrenceEsFieldMapper.getEsField(parameter), query))
      .aggregation(AggregationBuilders.terms(esField.getSearchFieldName()).field(esField.getExactMatchFieldName()).size(limit));

  }

  List<String> buildSuggestFullTextResponse(OccurrenceSearchParameter occurrenceSearchParameter, SearchResponse response) {
    return
    ((Terms)response.getAggregations().get(occurrenceEsFieldMapper.getSearchFieldName(occurrenceSearchParameter)))
      .getBuckets().stream()
      .map(Terms.Bucket::getKeyAsString)
      .collect(Collectors.toList());
  }

}
