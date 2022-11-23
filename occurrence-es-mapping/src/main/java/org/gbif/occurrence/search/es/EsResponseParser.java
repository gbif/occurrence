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

import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.search.Facet;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;

public class EsResponseParser<T extends VerbatimOccurrence> {

  // defaults
  private static final int DEFAULT_FACET_OFFSET = 0;
  private static final int DEFAULT_FACET_LIMIT = 10;

  private final EsFieldMapper esFieldMapper;
  private final Function<SearchHit,T> hitMapper;

  /**
   * Private constructor.
   */
  public EsResponseParser(EsFieldMapper esFieldMapper, Function<SearchHit,T> hitMapper) {
    this.esFieldMapper = esFieldMapper;
    this.hitMapper = hitMapper;
  }

  /**
   * Builds a SearchResponse instance using the current builder state.
   *
   * @return a new instance of a SearchResponse.
   */
  public SearchResponse<T, OccurrenceSearchParameter> buildSearchResponse(
      org.elasticsearch.action.search.SearchResponse esResponse, OccurrenceSearchRequest request) {

    SearchResponse<T, OccurrenceSearchParameter> response = new SearchResponse<>(request);
    response.setCount(esResponse.getHits().getTotalHits().value);
    parseHits(esResponse).ifPresent(response::setResults);
    parseFacets(esResponse, request).ifPresent(response::setFacets);

    return response;
  }

  public List<String> buildSuggestResponse(org.elasticsearch.action.search.SearchResponse esResponse,
                                                  OccurrenceSearchParameter parameter) {

    String fieldName = esFieldMapper.getValueFieldName(parameter);

    return esResponse.getSuggest().getSuggestion(fieldName).getEntries().stream()
        .flatMap(e -> ((CompletionSuggestion.Entry) e).getOptions().stream())
        .map(CompletionSuggestion.Entry.Option::getText)
        .map(Text::string)
        .collect(Collectors.toList());
  }

  /**
   * Extract the buckets of an {@link Aggregation}.
   */
  private List<? extends Terms.Bucket> getBuckets(Aggregation aggregation) {
    if (aggregation instanceof Terms) {
      return ((Terms) aggregation).getBuckets();
    } else if (aggregation instanceof Filter) {
      return
        ((Filter) aggregation)
          .getAggregations().asList()
            .stream()
            .flatMap(agg -> ((Terms) agg).getBuckets().stream())
            .collect(Collectors.toList());
    } else {
      throw new IllegalArgumentException(aggregation.getClass() + " aggregation not supported");
    }
  }

  private Optional<List<Facet<OccurrenceSearchParameter>>> parseFacets(
      org.elasticsearch.action.search.SearchResponse esResponse, OccurrenceSearchRequest request) {

    Function<Aggregation, Facet<OccurrenceSearchParameter>> mapFn = aggs -> {
      // get buckets
      List<? extends Terms.Bucket> buckets = getBuckets(aggs);

      // get facet of the agg
      OccurrenceSearchParameter facet =  esFieldMapper.getSearchParameter(aggs.getName());

      // check for paging in facets
      long facetOffset = extractFacetOffset(request, facet);
      long facetLimit = extractFacetLimit(request, facet);

      List<Facet.Count> counts =
        buckets.stream()
          .skip(facetOffset)
          .limit(facetOffset + facetLimit)
          .map(b -> new Facet.Count(b.getKeyAsString(), b.getDocCount()))
          .collect(Collectors.toList());

      return new Facet<>(facet, counts);
    };

    return Optional.ofNullable(esResponse.getAggregations())
      .map(aggregations -> aggregations.asList().stream().map(mapFn).collect(Collectors.toList()));
  }

  static int extractFacetLimit(OccurrenceSearchRequest request, OccurrenceSearchParameter facet) {
    return Optional.ofNullable(request.getFacetPage(facet))
      .map(Pageable::getLimit)
      .orElse(request.getFacetLimit() != null ? request.getFacetLimit() : DEFAULT_FACET_LIMIT);
  }

  static int extractFacetOffset(OccurrenceSearchRequest request, OccurrenceSearchParameter facet) {
    return Optional.ofNullable(request.getFacetPage(facet))
      .map(v -> (int) v.getOffset())
      .orElse(request.getFacetOffset() != null ? request.getFacetOffset() : DEFAULT_FACET_OFFSET);
  }

  private Optional<List<T>> parseHits(org.elasticsearch.action.search.SearchResponse esResponse) {
    if (esResponse.getHits() == null || esResponse.getHits().getHits() == null || esResponse.getHits().getHits().length == 0) {
      return Optional.empty();
    }

    return Optional.of(Stream.of(esResponse.getHits().getHits())
                         .map(hitMapper)
                         .collect(Collectors.toList()));
  }

}
