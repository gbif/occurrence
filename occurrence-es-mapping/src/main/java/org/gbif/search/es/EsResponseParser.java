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

import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.search.Facet;
import org.gbif.api.model.common.search.FacetedSearchRequest;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.VerbatimOccurrence;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.ChildrenAggregate;
import co.elastic.clients.elasticsearch._types.aggregations.FilterAggregate;
import co.elastic.clients.elasticsearch._types.aggregations.NestedAggregate;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch.core.search.CompletionSuggest;
import co.elastic.clients.elasticsearch.core.search.CompletionSuggestOption;
import co.elastic.clients.elasticsearch.core.search.Hit;

public abstract class EsResponseParser<
    T extends VerbatimOccurrence, P extends SearchParameter> {

  // defaults
  private static final int DEFAULT_FACET_OFFSET = 0;
  private static final int DEFAULT_FACET_LIMIT = 10;

  private final BaseEsFieldMapper<P> baseEsFieldMapper;
  private final Function<Hit<Map<String, Object>>, T> hitMapper;

  /** Private constructor. */
  public EsResponseParser(
      BaseEsFieldMapper<P> baseEsFieldMapper, Function<Hit<Map<String, Object>>, T> hitMapper) {
    this.baseEsFieldMapper = baseEsFieldMapper;
    this.hitMapper = hitMapper;
  }

  /**
   * Builds a SearchResponse instance using the current builder state.
   *
   * @return a new instance of a SearchResponse.
   */
  public SearchResponse<T, P> buildSearchResponse(
      co.elastic.clients.elasticsearch.core.SearchResponse<Map<String, Object>> esResponse,
      FacetedSearchRequest<P> request) {

    SearchResponse<T, P> response = new SearchResponse<>(request);
    response.setCount(
        Optional.ofNullable(esResponse.hits().total())
            .map(total -> total.value())
            .orElse((long) esResponse.hits().hits().size()));
    parseHits(esResponse).ifPresent(response::setResults);
    parseFacets(esResponse, request).ifPresent(response::setFacets);

    return response;
  }

  public List<String> buildSuggestResponse(
      co.elastic.clients.elasticsearch.core.SearchResponse<Map<String, Object>> esResponse,
      P parameter) {

    String fieldName = baseEsFieldMapper.getValueFieldName(parameter);
    if (esResponse.suggest() == null || !esResponse.suggest().containsKey(fieldName)) {
      return List.of();
    }
    return esResponse.suggest().get(fieldName).stream()
        .filter(s -> s.isCompletion())
        .map(s -> s.completion())
        .map(CompletionSuggest::options)
        .flatMap(List::stream)
        .map(CompletionSuggestOption::text)
        .collect(Collectors.toList());
  }

  /** Extract the buckets of an {@link Aggregate}. */
  private List<StringTermsBucket> getBuckets(Aggregate aggregation) {
    if (aggregation.isSterms()) {
      return aggregation.sterms().buckets().array();
    } else if (aggregation.isFilter()) {
      return toBucketList(aggregation.filter());
    } else if (aggregation.isChildren()) {
      return toBucketList(aggregation.children());
    } else if (aggregation.isNested()) {
      return toBucketList(aggregation.nested());
    } else {
      throw new IllegalArgumentException(aggregation.getClass() + " aggregation not supported");
    }
  }

  /** Extract the bucket list of a simple aggregation. */
  private static List<StringTermsBucket> toBucketList(FilterAggregate aggregation) {
    return toBucketList(aggregation.aggregations());
  }

  private static List<StringTermsBucket> toBucketList(ChildrenAggregate aggregation) {
    return toBucketList(aggregation.aggregations());
  }

  private static List<StringTermsBucket> toBucketList(NestedAggregate aggregation) {
    return toBucketList(aggregation.aggregations());
  }

  private static List<StringTermsBucket> toBucketList(Map<String, Aggregate> aggregations) {
    List<StringTermsBucket> buckets = new ArrayList<>();
    for (Aggregate agg : aggregations.values()) {
      if (agg.isFilter()) {
        buckets.addAll(toBucketList(agg.filter()));
      } else if (agg.isSterms()) {
        buckets.addAll(agg.sterms().buckets().array());
      }
    }
    return buckets;
  }

  private Optional<List<Facet<P>>> parseFacets(
      co.elastic.clients.elasticsearch.core.SearchResponse<Map<String, Object>> esResponse,
      FacetedSearchRequest<P> request) {

    Function<Map.Entry<String, Aggregate>, Facet<P>> mapFn =
        aggs -> {
          // get buckets
          List<StringTermsBucket> buckets = getBuckets(aggs.getValue());

          // get facet of the agg
          P facet = baseEsFieldMapper.getSearchParameter(aggs.getKey());
          if (facet == null) {
            facet = createSearchParameter(aggs.getKey(), String.class);
          }

          // check for paging in facets
          long facetOffset = extractFacetOffset(request, facet);
          long facetLimit = extractFacetLimit(request, facet);

          List<Facet.Count> counts =
              buckets.stream()
                  .skip(facetOffset)
                  .limit(facetOffset + facetLimit)
                  .map(b -> new Facet.Count(String.valueOf(b.key()), b.docCount()))
                  .collect(Collectors.toList());

          return new Facet<>(facet, counts);
        };

    return Optional.ofNullable(esResponse.aggregations())
        .map(aggregations -> aggregations.entrySet().stream().map(mapFn).collect(Collectors.toList()));
  }

  protected abstract P createSearchParameter(String name, Class<?> type);

  <R extends FacetedSearchRequest<P>> int extractFacetLimit(R request, P facet) {
    return Optional.ofNullable(request.getFacetPage(facet))
        .map(Pageable::getLimit)
        .orElse(request.getFacetLimit() != null ? request.getFacetLimit() : DEFAULT_FACET_LIMIT);
  }

  <R extends FacetedSearchRequest<P>>  int extractFacetOffset(R request, P facet) {
    return Optional.ofNullable(request.getFacetPage(facet))
        .map(v -> (int) v.getOffset())
        .orElse(request.getFacetOffset() != null ? request.getFacetOffset() : DEFAULT_FACET_OFFSET);
  }

  private Optional<List<T>> parseHits(
      co.elastic.clients.elasticsearch.core.SearchResponse<Map<String, Object>> esResponse) {
    if (esResponse.hits() == null
        || esResponse.hits().hits() == null
        || esResponse.hits().hits().isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(esResponse.hits().hits().stream().map(hitMapper).collect(Collectors.toList()));
  }
}
