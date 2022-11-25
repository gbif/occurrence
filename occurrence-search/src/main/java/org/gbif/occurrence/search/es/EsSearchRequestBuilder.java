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

import org.gbif.api.model.common.search.SearchConstants;
import org.gbif.api.model.occurrence.geo.DistanceUnit;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.Country;
import org.gbif.occurrence.search.predicate.OccurrencePredicateSearchRequest;
import org.gbif.occurrence.search.predicate.QueryVisitorFactory;
import org.gbif.predicate.query.EsQueryVisitor;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.*;
import org.elasticsearch.index.query.*;
import org.elasticsearch.join.aggregations.ChildrenAggregationBuilder;
import org.elasticsearch.join.query.JoinQueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import com.google.common.annotations.VisibleForTesting;

import lombok.SneakyThrows;

import static org.gbif.api.util.SearchTypeValidator.isRange;
import static org.gbif.occurrence.search.es.EsQueryUtils.*;
import static org.gbif.occurrence.search.es.OccurrenceEsField.COORDINATE_POINT;
import static org.gbif.occurrence.search.es.OccurrenceEsField.COORDINATE_SHAPE;
import static org.gbif.occurrence.search.es.OccurrenceEsField.FULL_TEXT;

public class EsSearchRequestBuilder {

  private static final int MAX_SIZE_TERMS_AGGS = 1200000;
  private static final IntUnaryOperator DEFAULT_SHARD_SIZE = size -> (size * 2) + 50000;

  public static String[] SOURCE_EXCLUDE = new String[]{"all", "notIssues", "*.verbatim", "*.suggest"};

  private final EsFieldMapper esFieldMapper;

  public EsSearchRequestBuilder(EsFieldMapper esFieldMapper) {
    this.esFieldMapper = esFieldMapper;
  }

  @SneakyThrows
  public Optional<BoolQueryBuilder> buildQuery(OccurrencePredicateSearchRequest searchRequest) {
    // create bool node
    BoolQueryBuilder bool = QueryBuilders.boolQuery();
    String qParam = searchRequest.getQ();

    // adding full text search parameter
    if (!Strings.isNullOrEmpty(qParam)) {
      bool.must(QueryBuilders.matchQuery(esFieldMapper.getSearchFieldName(FULL_TEXT), qParam));
    }

    EsQueryVisitor<OccurrenceSearchParameter> esQueryVisitor = QueryVisitorFactory.createEsQueryVisitor(esFieldMapper.getSearchType(), esFieldMapper.isNestedIndex());
    esQueryVisitor.getQueryBuilder(searchRequest.getPredicate()).ifPresent(bool::must);

    return bool.must().isEmpty() && bool.filter().isEmpty() ? Optional.empty() : Optional.of(bool);
  }

  public SearchRequest buildSearchRequest(
      OccurrenceSearchRequest searchRequest, String index) {

    SearchRequest esRequest = new SearchRequest();
    esRequest.indices(index);

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    esRequest.source(searchSourceBuilder);

    // size and offset
    searchSourceBuilder.size(searchRequest.getLimit());
    searchSourceBuilder.from((int) searchRequest.getOffset());
    searchSourceBuilder.trackTotalHits(true);
    searchSourceBuilder.fetchSource(null , SOURCE_EXCLUDE);

    // sort
    if (Strings.isNullOrEmpty(searchRequest.getQ())) {
      searchSourceBuilder.sort(SortBuilders.fieldSort(esFieldMapper.getSearchFieldName(OccurrenceEsField.YEAR)).order(SortOrder.DESC));
      searchSourceBuilder.sort(SortBuilders.fieldSort(esFieldMapper.getSearchFieldName(OccurrenceEsField.MONTH)).order(SortOrder.ASC));
      searchSourceBuilder.sort(SortBuilders.fieldSort(esFieldMapper.getSearchType() == EsFieldMapper.SearchType.OCCURRENCE? "gbifId" : "uniqueKey").order(SortOrder.ASC));
    } else {
      searchSourceBuilder.sort(SortBuilders.scoreSort());
    }

    // group params
    GroupedParams groupedParams = groupParameters(searchRequest);

    // add query
    if (searchRequest instanceof OccurrencePredicateSearchRequest) {
      buildQuery((OccurrencePredicateSearchRequest)searchRequest).ifPresent(
        searchSourceBuilder::query);
    } else {
      buildQuery(groupedParams.queryParams, searchRequest.getQ(), searchRequest.isMatchCase()).ifPresent(
        searchSourceBuilder::query);
    }

    // add aggs
    buildAggs(searchRequest, groupedParams.postFilterParams)
        .ifPresent(aggsList -> aggsList.forEach(searchSourceBuilder::aggregation));

    // post-filter
    buildPostFilter(groupedParams.postFilterParams, searchRequest.isMatchCase()).ifPresent(searchSourceBuilder::postFilter);

    return esRequest;
  }

  public Optional<QueryBuilder> buildQueryNode(OccurrenceSearchRequest searchRequest) {
    return buildQuery(searchRequest.getParameters(), searchRequest.getQ(), searchRequest.isMatchCase());
  }

  SearchRequest buildSuggestQuery(
      String prefix, OccurrenceSearchParameter parameter, Integer limit, String index) {
    SearchRequest request = new SearchRequest();
    request.indices(index);

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    request.source(searchSourceBuilder);

    OccurrenceEsField esField = esFieldMapper.getOccurrenceEsField(parameter);

    // create suggest query
    searchSourceBuilder.suggest(
        new SuggestBuilder()
            .addSuggestion(
                esFieldMapper.getSearchFieldName(esField),
                SuggestBuilders.completionSuggestion(esFieldMapper.getSuggestFieldName(esField))
                    .prefix(prefix)
                    .size(limit != null ? limit : SearchConstants.DEFAULT_SUGGEST_LIMIT)
                    .skipDuplicates(true)));

    // add source field
    searchSourceBuilder.fetchSource(esFieldMapper.getSearchFieldName(esField), null);

    return request;
  }

  private Optional<QueryBuilder> buildQuery(
      Map<OccurrenceSearchParameter, Set<String>> params, String qParam, boolean matchCase) {
    // create bool node
    BoolQueryBuilder bool = QueryBuilders.boolQuery();

    // adding full text search parameter
    if (!Strings.isNullOrEmpty(qParam)) {
      bool.must(QueryBuilders.matchQuery(esFieldMapper.getSearchFieldName(FULL_TEXT), qParam));
    }

    if (params != null && !params.isEmpty()) {
      // adding geometry to bool
      if (params.containsKey(OccurrenceSearchParameter.GEOMETRY)) {
        BoolQueryBuilder shouldGeometry = QueryBuilders.boolQuery();
        shouldGeometry
            .should()
            .addAll(
                params.get(OccurrenceSearchParameter.GEOMETRY).stream()
                    .map(this::buildGeoShapeQuery)
                    .collect(Collectors.toList()));
        bool.filter().add(shouldGeometry);
      }

      if (params.containsKey(OccurrenceSearchParameter.GEO_DISTANCE)) {
        BoolQueryBuilder shouldGeoDistance = QueryBuilders.boolQuery();
        shouldGeoDistance
          .should()
          .addAll(
            params.get(OccurrenceSearchParameter.GEO_DISTANCE).stream()
              .map(this::buildGeoDistanceQuery)
              .collect(Collectors.toList()));
        bool.filter().add(shouldGeoDistance);
      }

      // adding term queries to bool
      bool.filter()
          .addAll(
              params.entrySet().stream()
                  .filter(e -> Objects.nonNull(esFieldMapper.getOccurrenceEsField(e.getKey())))
                  .flatMap(
                      e ->
                          buildTermQuery(e.getValue(), e.getKey(), esFieldMapper.getOccurrenceEsField(e.getKey()), matchCase)
                              .stream())
                  .collect(Collectors.toList()));
    }
    if (esFieldMapper.isNestedIndex()) {
      bool.filter().add(QueryBuilders.termQuery("type", esFieldMapper.getSearchType().getObjectName()));
    }
    return bool.must().isEmpty() && bool.filter().isEmpty() ? Optional.empty() : Optional.of(bool);
  }

  @VisibleForTesting
  static GroupedParams groupParameters(OccurrenceSearchRequest searchRequest) {
    GroupedParams groupedParams = new GroupedParams();

    if (searchRequest.getFacets() == null
        || searchRequest.getFacets().isEmpty()) {
      groupedParams.queryParams = searchRequest.getParameters();
      return groupedParams;
    }

    groupedParams.queryParams = new HashMap<>();
    groupedParams.postFilterParams = new HashMap<>();

    searchRequest
        .getParameters()
        .forEach(
            (k, v) -> {
              if (searchRequest.getFacets().contains(k)) {
                groupedParams.postFilterParams.put(k, v);
              } else {
                groupedParams.queryParams.put(k, v);
              }
            });

    return groupedParams;
  }

  private Optional<QueryBuilder> buildPostFilter(
      Map<OccurrenceSearchParameter, Set<String>> postFilterParams, boolean matchCase) {
    if (postFilterParams == null || postFilterParams.isEmpty()) {
      return Optional.empty();
    }

    BoolQueryBuilder bool = QueryBuilders.boolQuery();
    bool.filter()
        .addAll(
            postFilterParams.entrySet().stream()
                .flatMap(
                    e ->
                        buildTermQuery(e.getValue(), e.getKey(), esFieldMapper.getOccurrenceEsField(e.getKey()), matchCase)
                          .stream())
                          .collect(Collectors.toList()));

    return Optional.of(bool);
  }

  private Optional<List<AggregationBuilder>> buildAggs(
      OccurrenceSearchRequest searchRequest,
      Map<OccurrenceSearchParameter, Set<String>> postFilterParams) {
    if (searchRequest.getFacets() == null || searchRequest.getFacets().isEmpty()) {
      return Optional.empty();
    }

    if (searchRequest.isMultiSelectFacets()
        && postFilterParams != null
        && !postFilterParams.isEmpty()) {
      return Optional.of(buildFacetsMultiselect(searchRequest, postFilterParams));
    }

    return Optional.of(buildFacets(searchRequest, postFilterParams));
  }

  /** Creates a filter with all the filter in which the facet param is not present.*/
  private BoolQueryBuilder getAggregationPostFilter(Map<OccurrenceSearchParameter, Set<String>> postFilterParams, OccurrenceSearchParameter facetParam, boolean matchCase) {
    BoolQueryBuilder bool = QueryBuilders.boolQuery();
    bool.filter()
      .addAll(
        postFilterParams.entrySet().stream()
          .filter(entry -> entry.getKey() != facetParam)
          .flatMap(
            e ->
              buildTermQuery(
                e.getValue(),
                e.getKey(),
                esFieldMapper.getOccurrenceEsField(e.getKey()),
                matchCase)
                .stream())
          .collect(Collectors.toList()));
    return bool;
  }

  /** Creates a filter with all the filter in which the facet param is not present.*/
  private BoolQueryBuilder getAggregationFilter(Map<OccurrenceSearchParameter, Set<String>> postFilterParams, OccurrenceSearchParameter facetParam, boolean matchCase, boolean wrappedChildrenQueries) {
    BoolQueryBuilder bool = QueryBuilders.boolQuery();
    bool.filter()
        .addAll(buildTermQuery(postFilterParams.get(facetParam),
                               facetParam,
                               esFieldMapper.getOccurrenceEsField(facetParam),
                               matchCase,
                               wrappedChildrenQueries));
    return bool;
  }

  private List<AggregationBuilder> buildFacetsMultiselect(
      OccurrenceSearchRequest searchRequest,
      Map<OccurrenceSearchParameter, Set<String>> postFilterParams) {

    if (searchRequest.getFacets().size() == 1) {
      // same case as normal facets
      return buildFacets(searchRequest, postFilterParams);
    }

    return searchRequest.getFacets().stream()
        .filter(p -> esFieldMapper.getOccurrenceEsField(p) != null)
        .map(
            facetParam -> {

              // build filter aggs
              BoolQueryBuilder bool = getAggregationPostFilter(postFilterParams, facetParam, searchRequest.isMatchCase());

              // add filter to the aggs
              OccurrenceEsField esField = esFieldMapper.getOccurrenceEsField(facetParam);
              FilterAggregationBuilder filterAggs =
                  AggregationBuilders.filter(esFieldMapper.getSearchFieldName(esField), bool);

              // build terms aggs and add it to the filter aggs
              TermsAggregationBuilder termsAggs =
                  buildTermsAggs(
                      "filtered_" + esFieldMapper.getSearchFieldName(esField),
                      esField,
                      searchRequest,
                      facetParam);
              if(esFieldMapper.isChildField(esField)) {
                filterAggs.subAggregation(getChildrenAggregationBuilder(searchRequest, postFilterParams, facetParam, esField));
              } else {
                filterAggs.subAggregation(termsAggs);
              }

              return filterAggs;
            })
        .collect(Collectors.toList());
  }

  private List<AggregationBuilder> buildFacets(OccurrenceSearchRequest searchRequest, Map<OccurrenceSearchParameter, Set<String>> postFilterParams) {
    return searchRequest.getFacets().stream()
        .filter(p -> EsFieldMapper.SEARCH_TO_ES_MAPPING.get(p) != null)
        .map(
            facetParam -> {
              OccurrenceEsField esField = esFieldMapper.getOccurrenceEsField(facetParam);
              if (esFieldMapper.isChildField(esField)) {
                return getChildrenAggregationBuilder(searchRequest, postFilterParams, facetParam, esField);
              }
              return buildTermsAggs(esFieldMapper.getSearchFieldName(esField), esField, searchRequest, facetParam);
            })
        .collect(Collectors.toList());
  }

  private ChildrenAggregationBuilder getChildrenAggregationBuilder(
    OccurrenceSearchRequest searchRequest,
    Map<OccurrenceSearchParameter, Set<String>> postFilterParams,
    OccurrenceSearchParameter facetParam,
    OccurrenceEsField esField
  ) {
    return new ChildrenAggregationBuilder(esFieldMapper.getSearchFieldName(esField), esFieldMapper.getChildRelation(
      esField))
                              .subAggregation(
                                AggregationBuilders.filter(esFieldMapper.getSearchFieldName(esField), getAggregationFilter(
                                    postFilterParams,
                                    facetParam, searchRequest.isMatchCase(), false))
                                  .subAggregation(buildTermsAggs(esFieldMapper.getSearchFieldName(esField),
                                                                 esField,
                                                                 searchRequest,
                                                                 facetParam)));
  }

  private TermsAggregationBuilder buildTermsAggs(
      String aggName,
      OccurrenceEsField esField,
      OccurrenceSearchRequest searchRequest,
      OccurrenceSearchParameter facetParam) {
    // build aggs for the field
    TermsAggregationBuilder termsAggsBuilder =
        AggregationBuilders.terms(aggName)
          .field(searchRequest.isMatchCase()? esFieldMapper.getVerbatimFieldName(esField) : esFieldMapper.getExactMatchFieldName(esField));

    // min count
    Optional.ofNullable(searchRequest.getFacetMinCount()).ifPresent(termsAggsBuilder::minDocCount);

    // aggs size
    int size = calculateAggsSize(esField, extractFacetOffset(searchRequest, facetParam), extractFacetLimit(searchRequest, facetParam));
    termsAggsBuilder.size(size);

    // aggs shard size
    termsAggsBuilder.shardSize(
        CARDINALITIES.getOrDefault(esField, DEFAULT_SHARD_SIZE.applyAsInt(size)));

    return termsAggsBuilder;
  }

  private static int calculateAggsSize(OccurrenceEsField esField, int facetOffset, int facetLimit) {
    int maxCardinality = CARDINALITIES.getOrDefault(esField, Integer.MAX_VALUE);

    // the limit is bounded by the max cardinality of the field
    int limit = Math.min(facetOffset + facetLimit, maxCardinality);

    // we set a maximum limit for performance reasons
    if (limit > MAX_SIZE_TERMS_AGGS) {
      throw new IllegalArgumentException(
          "Facets paging is only supported up to " + MAX_SIZE_TERMS_AGGS + " elements");
    }
    return limit;
  }

  /**
   * Mapping parameter values into know values for Enums. Non-enum parameter values are passed using
   * its raw value.
   */
  private String parseParamValue(String value, OccurrenceSearchParameter parameter) {
    if (Enum.class.isAssignableFrom(parameter.type())
        && !Country.class.isAssignableFrom(parameter.type())) {
      return VocabularyUtils.lookup(value, (Class<Enum<?>>) parameter.type())
          .map(Enum::name)
          .orElse(null);
    }
    if (Boolean.class.isAssignableFrom(parameter.type())) {
      return value.toLowerCase();
    }
    return value;
  }

  private List<QueryBuilder> buildTermQuery(Collection<String> values, OccurrenceSearchParameter param, OccurrenceEsField esField, boolean matchCase) {
    return buildTermQuery(values, param, esField, matchCase, true);
  }

  private List<QueryBuilder> buildTermQuery(
      Collection<String> values, OccurrenceSearchParameter param, OccurrenceEsField esField,
      boolean matchCase, boolean wrappedChildrenQueries) {
    List<QueryBuilder> queries = new ArrayList<>();

    // collect queries for each value
    List<String> parsedValues = new ArrayList<>();
    for (String value : values) {
      if (isRange(value)) {
        queries.add(buildRangeQuery(esField, value));
        continue;
      }

      parsedValues.add(parseParamValue(value, param));
    }

    String fieldName = matchCase? esFieldMapper.getVerbatimFieldName(esField) : esFieldMapper.getExactMatchFieldName(esField);
    if (parsedValues.size() == 1) {
      // single term
      queries.add(QueryBuilders.termQuery(fieldName, parsedValues.get(0)));
    } else if (parsedValues.size() > 1) {
      // multi term query
      queries.add(QueryBuilders.termsQuery(fieldName, parsedValues));
    }

    if (wrappedChildrenQueries && esFieldMapper.isChildField(esField)) {
      return queries.stream()
              .map(q -> JoinQueryBuilders.hasChildQuery(esFieldMapper.getChildRelation(esField), q, ScoreMode.None))
              .collect(Collectors.toList());

    }
    return queries;
  }

  private RangeQueryBuilder buildRangeQuery(OccurrenceEsField esField, String value) {
    RangeQueryBuilder builder = QueryBuilders.rangeQuery(esFieldMapper.getExactMatchFieldName(esField));

    String[] values = value.split(RANGE_SEPARATOR);
    if (DATE_FIELDS.contains(esField)) {

      LocalDateTime lowerBound = LOWER_BOUND_RANGE_PARSER.apply(values[0]);
      if (lowerBound != null) {
        builder.gte(lowerBound);
      }

      LocalDateTime upperBound = UPPER_BOUND_RANGE_PARSER.apply(values[1]);
      if (upperBound != null) {
        builder.lte(upperBound);
      }
    } else {
      if (!RANGE_WILDCARD.equals(values[0])) {
        builder.gte(values[0]);
      }
      if (!RANGE_WILDCARD.equals(values[1])) {
        builder.lte(values[1]);
      }
    }

    return builder;
  }

  public GeoDistanceQueryBuilder buildGeoDistanceQuery(String rawGeoDistance) {
    DistanceUnit.GeoDistance geoDistance = DistanceUnit.GeoDistance.parseGeoDistance(rawGeoDistance);
    return buildGeoDistanceQuery(geoDistance);
  }

  public GeoDistanceQueryBuilder buildGeoDistanceQuery(DistanceUnit.GeoDistance geoDistance) {
    return QueryBuilders.geoDistanceQuery(esFieldMapper.getSearchFieldName(COORDINATE_POINT))
      .distance(geoDistance.getDistance().toString())
      .point(geoDistance.getLatitude(), geoDistance.getLongitude());
  }

  public GeoShapeQueryBuilder buildGeoShapeQuery(String wkt) {
    Geometry geometry;
    try {
      geometry = new WKTReader().read(wkt);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }

    Function<Polygon, PolygonBuilder> polygonToBuilder =
        polygon -> {
          PolygonBuilder polygonBuilder =
              new PolygonBuilder(
                  new CoordinatesBuilder()
                      .coordinates(
                          normalizePolygonCoordinates(polygon.getExteriorRing().getCoordinates())));
          for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            polygonBuilder.hole(
                new LineStringBuilder(
                    new CoordinatesBuilder()
                        .coordinates(
                            normalizePolygonCoordinates(
                                polygon.getInteriorRingN(i).getCoordinates()))));
          }
          return polygonBuilder;
        };

    String type =
        "LinearRing".equals(geometry.getGeometryType())
            ? "LINESTRING"
            : geometry.getGeometryType().toUpperCase();

    ShapeBuilder shapeBuilder = null;
    if (("POINT").equals(type)) {
      shapeBuilder = new PointBuilder(geometry.getCoordinate().x, geometry.getCoordinate().y);
    } else if ("LINESTRING".equals(type)) {
      shapeBuilder = new LineStringBuilder(Arrays.asList(geometry.getCoordinates()));
    } else if ("POLYGON".equals(type)) {
      shapeBuilder = polygonToBuilder.apply((Polygon) geometry);
    } else if ("MULTIPOLYGON".equals(type)) {
      // multipolygon
      MultiPolygonBuilder multiPolygonBuilder = new MultiPolygonBuilder();
      for (int i = 0; i < geometry.getNumGeometries(); i++) {
        multiPolygonBuilder.polygon(polygonToBuilder.apply((Polygon) geometry.getGeometryN(i)));
      }
      shapeBuilder = multiPolygonBuilder;
    } else {
      throw new IllegalArgumentException(type + " shape is not supported");
    }

    try {
      return QueryBuilders.geoShapeQuery(esFieldMapper.getSearchFieldName(COORDINATE_SHAPE), shapeBuilder.buildGeometry())
          .relation(ShapeRelation.WITHIN);
    } catch (IOException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }

  /** Eliminates consecutive duplicates. The order is preserved. */
  @VisibleForTesting
  static Coordinate[] normalizePolygonCoordinates(Coordinate[] coordinates) {
    List<Coordinate> normalizedCoordinates = new ArrayList<>();

    // we always have to keep the fist and last coordinates
    int i = 0;
    normalizedCoordinates.add(i++, coordinates[0]);

    for (int j = 1; j < coordinates.length; j++) {
      if (!coordinates[j - 1].equals(coordinates[j])) {
        normalizedCoordinates.add(i++, coordinates[j]);
      }
    }

    return normalizedCoordinates.toArray(new Coordinate[0]);
  }

  @VisibleForTesting
  static class GroupedParams {
    Map<OccurrenceSearchParameter, Set<String>> postFilterParams;
    Map<OccurrenceSearchParameter, Set<String>> queryParams;
  }
}
