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
import lombok.Data;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.predicate.query.EsField;
import org.gbif.search.es.occurrence.OccurrenceEsFieldMapper;

@Data
public class EsFulltextSuggestBuilder {

  private final OccurrenceEsFieldMapper occurrenceEsFieldMapper;

  public EsFulltextSuggestBuilder(OccurrenceEsFieldMapper occurrenceEsFieldMapper) {
    this.occurrenceEsFieldMapper = occurrenceEsFieldMapper;
  }

  private static boolean isPhraseQuery(String query) {
    return query.contains(" ");
  }

  private Query buildSuggestQuery(EsField esField, String query) {
    String exactField = esField.getExactMatchFieldName();
    String searchField = esField.getSearchFieldName();

    Query baseQuery = Query.of(q -> q.bool(b -> b
        .should(s -> s.prefix(p -> p.field(exactField).value(query)))
        .should(s -> isPhraseQuery(query)
            ? s.matchPhrase(m -> m.field(searchField).query(query))
            : s.match(m -> m.field(searchField).query(v -> v.stringValue(query))))
        .minimumShouldMatch("1")));

    return occurrenceEsFieldMapper.getDefaultFilter()
        .map(filter -> Query.of(q -> q.bool(b -> b.must(baseQuery).filter(filter))))
        .orElse(baseQuery);
  }

  /**
   * Builds a full suggest search request using the new Elasticsearch API.
   *
   * @param query     user input for suggest
   * @param parameter search parameter (field) to suggest on
   * @param limit     max number of suggestions
   * @param index     index name to search
   * @return SearchRequest for the new ElasticsearchClient
   */
  public SearchRequest buildSuggestFullTextRequest(
      String query,
      OccurrenceSearchParameter parameter,
      Integer limit,
      String index) {
    EsField esField = occurrenceEsFieldMapper.getEsField(parameter);
    String aggName = occurrenceEsFieldMapper.getSearchFieldName(parameter);

    return SearchRequest.of(s -> s
        .index(index)
        .size(0)
        .query(buildSuggestQuery(esField, query))
        .aggregations(aggName, a -> a.terms(t -> t
            .field(esField.getExactMatchFieldName())
            .size(limit != null ? limit : 10))));
  }

  public List<String> buildSuggestFullTextResponse(
      OccurrenceSearchParameter occurrenceSearchParameter,
      SearchResponse<?> response) {
    String aggName = occurrenceEsFieldMapper.getSearchFieldName(occurrenceSearchParameter);
    if (response.aggregations() == null) {
      return List.of();
    }
    var agg = response.aggregations().get(aggName);
    if (agg == null || !agg.isSterms()) {
      return List.of();
    }
    return agg.sterms().buckets().array().stream()
        .map(b -> b.key().stringValue())
        .collect(Collectors.toList());
  }
}
