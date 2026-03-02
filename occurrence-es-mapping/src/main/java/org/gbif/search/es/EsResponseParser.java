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
package org.gbif.search.es;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch.core.search.Suggestion;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.search.Facet;
import org.gbif.api.model.common.search.FacetedSearchRequest;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.VerbatimOccurrence;

public abstract class EsResponseParser<
    T extends VerbatimOccurrence, P extends SearchParameter> {

  // defaults
  private static final int DEFAULT_FACET_OFFSET = 0;
  private static final int DEFAULT_FACET_LIMIT = 10;

  private final BaseEsFieldMapper<P> baseEsFieldMapper;
  private final Function<Hit<Map<String, Object>>, T> hitMapper;

  /** Constructor. */
  public EsResponseParser(
      BaseEsFieldMapper<P> baseEsFieldMapper, Function<Hit<Map<String, Object>>, T> hitMapper) {
    this.baseEsFieldMapper = baseEsFieldMapper;
    this.hitMapper = hitMapper;
  }

  /**
   * Builds a SearchResponse instance using the new Elasticsearch Java API client response.
   */
  public SearchResponse<T, P> buildSearchResponse(
      co.elastic.clients.elasticsearch.core.SearchResponse<Map<String, Object>> esResponse,
      FacetedSearchRequest<P> request) {

    SearchResponse<T, P> response = new SearchResponse<>(request);
    long total = esResponse.hits().total() != null ? esResponse.hits().total().value() : 0L;
    response.setCount(total);
    parseHits(esResponse).ifPresent(response::setResults);
    parseFacets(esResponse, request).ifPresent(response::setFacets);

    return response;
  }

  /** Build suggest response from new client's SearchResponse. */
  public List<String> buildSuggestResponse(
      co.elastic.clients.elasticsearch.core.SearchResponse<Map<String, Object>> esResponse,
      P parameter) {

    String fieldName = baseEsFieldMapper.getValueFieldName(parameter);
    Map<String, List<Suggestion<Map<String, Object>>>> suggest = esResponse.suggest();
    if (suggest == null) {
      return Collections.emptyList();
    }
    List<Suggestion<Map<String, Object>>> fieldSuggest = suggest.get(fieldName);
    if (fieldSuggest == null || fieldSuggest.isEmpty()) {
      return Collections.emptyList();
    }
    return fieldSuggest.stream()
        .filter(Suggestion::isCompletion)
        .flatMap(s -> s.completion().options().stream())
        .map(o -> o.text())
        .filter(Objects::nonNull)
        .toList();
  }

  private Optional<List<Facet<P>>> parseFacets(
      co.elastic.clients.elasticsearch.core.SearchResponse<Map<String, Object>> esResponse,
      FacetedSearchRequest<P> request) {

    Map<String, Aggregate> aggregations = esResponse.aggregations();
    if (aggregations == null || aggregations.isEmpty()) {
      return Optional.empty();
    }

    List<Facet<P>> facets = aggregations.entrySet().stream()
        .map(entry -> toFacet(entry.getKey(), entry.getValue(), request))
        .collect(Collectors.toList());
    return Optional.of(facets);
  }

  private Facet<P> toFacet(String aggName, Aggregate agg, FacetedSearchRequest<P> request) {
    P facet = baseEsFieldMapper.getSearchParameter(aggName);
    if (facet == null) {
      facet = createSearchParameter(aggName, String.class);
    }
    long facetOffset = extractFacetOffset(request, facet);
    long facetLimit = extractFacetLimit(request, facet);

    List<Facet.Count> counts = getCountsFromAggregate(agg);
    counts = counts.stream()
        .skip(facetOffset)
        .limit(facetLimit)
        .toList();

    return new Facet<>(facet, counts);
  }

  private List<Facet.Count> getCountsFromAggregate(Aggregate agg) {
    if (agg.isSterms()) {
      return agg.sterms().buckets().array().stream()
          .map(b -> new Facet.Count(b.key().stringValue(), b.docCount()))
          .toList();
    }
    if (agg.isLterms()) {
      return agg.lterms().buckets().array().stream()
          .map(b -> new Facet.Count(String.valueOf(b.key()), b.docCount()))
          .toList();
    }
    if (agg.isFilter()) {
      return Collections.emptyList();
    }
    if (agg.isNested()) {
      return getCountsFromNestedAggregate(agg.nested());
    }
    return Collections.emptyList();
  }

  private List<Facet.Count> getCountsFromNestedAggregate(
      co.elastic.clients.elasticsearch._types.aggregations.NestedAggregate nested) {
    Map<String, Aggregate> sub = nested.aggregations();
    if (sub == null || sub.isEmpty()) {
      return Collections.emptyList();
    }
    for (Aggregate a : sub.values()) {
      List<Facet.Count> c = getCountsFromAggregate(a);
      if (!c.isEmpty()) {
        return c;
      }
    }
    return Collections.emptyList();
  }

  protected abstract P createSearchParameter(String name, Class<?> type);

  <R extends FacetedSearchRequest<P>> int extractFacetLimit(R request, P facet) {
    return Optional.ofNullable(request.getFacetPage(facet))
        .map(Pageable::getLimit)
        .orElse(request.getFacetLimit() != null ? request.getFacetLimit() : DEFAULT_FACET_LIMIT);
  }

  <R extends FacetedSearchRequest<P>> int extractFacetOffset(R request, P facet) {
    return Optional.ofNullable(request.getFacetPage(facet))
        .map(v -> (int) v.getOffset())
        .orElse(request.getFacetOffset() != null ? request.getFacetOffset() : DEFAULT_FACET_OFFSET);
  }

  private Optional<List<T>> parseHits(
      co.elastic.clients.elasticsearch.core.SearchResponse<Map<String, Object>> esResponse) {
    if (esResponse.hits() == null || esResponse.hits().hits() == null
        || esResponse.hits().hits().isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(
        esResponse.hits().hits().stream()
            .map(hitMapper)
            .toList());
  }
}
