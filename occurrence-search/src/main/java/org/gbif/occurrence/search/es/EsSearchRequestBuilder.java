package org.gbif.occurrence.search.es;

import org.gbif.api.model.common.search.SearchConstants;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;

import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.*;
import org.elasticsearch.index.query.*;
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

import static org.gbif.api.util.SearchTypeValidator.isRange;
import static org.gbif.occurrence.search.es.EsQueryUtils.*;
import static org.gbif.occurrence.search.es.OccurrenceEsField.COORDINATE_SHAPE;
import static org.gbif.occurrence.search.es.OccurrenceEsField.FULL_TEXT;

public class EsSearchRequestBuilder {

  private static final int MAX_SIZE_TERMS_AGGS = 20000;

  private EsSearchRequestBuilder() {}

  public static SearchRequest buildSearchRequest(
      OccurrenceSearchRequest searchRequest,
      boolean facetsEnabled,
      int maxOffset,
      int maxLimit,
      String index) {

    SearchRequest esRequest = new SearchRequest();
    esRequest.indices(index);

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    esRequest.source(searchSourceBuilder);

    // size and offset
    searchSourceBuilder.size(Math.min(searchRequest.getLimit(), maxLimit));
    searchSourceBuilder.from((int) Math.min(maxOffset, searchRequest.getOffset()));

    // sort
    if (Strings.isNullOrEmpty(searchRequest.getQ())) {
      searchSourceBuilder.sort(SortBuilders.fieldSort("_doc").order(SortOrder.DESC));
    } else {
      searchSourceBuilder.sort(SortBuilders.scoreSort());
    }

    // group params
    GroupedParams groupedParams = groupParameters(searchRequest);

    // add query
    buildQuery(groupedParams.queryParams, searchRequest.getQ())
        .ifPresent(searchSourceBuilder::query);

    // add aggs
    buildAggs(searchRequest, groupedParams.postFilterParams, facetsEnabled)
        .ifPresent(aggsList -> aggsList.forEach(searchSourceBuilder::aggregation));

    // post-filter
    buildPostFilter(groupedParams.postFilterParams).ifPresent(searchSourceBuilder::postFilter);

    return esRequest;
  }

  public static Optional<QueryBuilder> buildQueryNode(OccurrenceSearchRequest searchRequest) {
    return buildQuery(searchRequest.getParameters(), searchRequest.getQ());
  }

  public static SearchRequest buildSuggestQuery(
      String prefix, OccurrenceSearchParameter parameter, Integer limit, String index) {
    SearchRequest request = new SearchRequest();
    request.indices(index);

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    request.source(searchSourceBuilder);

    OccurrenceEsField esField = SEARCH_TO_ES_MAPPING.get(parameter);

    // create suggest query
    searchSourceBuilder.suggest(
        new SuggestBuilder()
            .addSuggestion(
                esField.getFieldName(),
                SuggestBuilders.completionSuggestion(esField.getFieldName() + ".suggest")
                    .prefix(prefix)
                    .size(limit != null ? limit : SearchConstants.DEFAULT_SUGGEST_LIMIT)
                    .skipDuplicates(true)));

    // add source field
    searchSourceBuilder.fetchSource(esField.getFieldName(), null);

    return request;
  }

  private static Optional<QueryBuilder> buildQuery(
      Multimap<OccurrenceSearchParameter, String> params, String qParam) {
    // create bool node
    BoolQueryBuilder bool = QueryBuilders.boolQuery();

    // adding full text search parameter
    if (!Strings.isNullOrEmpty(qParam)) {
      bool.must(QueryBuilders.matchQuery(FULL_TEXT.getFieldName(), qParam));
    }

    if (params != null && !params.isEmpty()) {
      // adding geometry to bool
      if (params.containsKey(OccurrenceSearchParameter.GEOMETRY)) {
        BoolQueryBuilder shouldGeometry = QueryBuilders.boolQuery();
        shouldGeometry
            .should()
            .addAll(
                params.get(OccurrenceSearchParameter.GEOMETRY).stream()
                    .map(EsSearchRequestBuilder::buildGeoShapeQuery)
                    .collect(Collectors.toList()));
        bool.filter().add(shouldGeometry);
      }

      // adding term queries to bool
      bool.filter()
          .addAll(
              params.asMap().entrySet().stream()
                  .filter(e -> Objects.nonNull(SEARCH_TO_ES_MAPPING.get(e.getKey())))
                  .flatMap(
                      e ->
                          buildTermQuery(
                              e.getValue(), e.getKey(), SEARCH_TO_ES_MAPPING.get(e.getKey()))
                              .stream())
                  .collect(Collectors.toList()));
    }

    return bool.must().isEmpty() && bool.filter().isEmpty() ? Optional.empty() : Optional.of(bool);
  }

  @VisibleForTesting
  static GroupedParams groupParameters(OccurrenceSearchRequest searchRequest) {
    GroupedParams groupedParams = new GroupedParams();

    if (!searchRequest.isMultiSelectFacets()
        || searchRequest.getFacets() == null
        || searchRequest.getFacets().isEmpty()) {
      groupedParams.queryParams = searchRequest.getParameters();
      return groupedParams;
    }

    groupedParams.queryParams = ArrayListMultimap.create();
    groupedParams.postFilterParams = ArrayListMultimap.create();

    searchRequest
        .getParameters()
        .asMap()
        .forEach(
            (k, v) -> {
              if (searchRequest.getFacets().contains(k)) {
                groupedParams.postFilterParams.putAll(k, v);
              } else {
                groupedParams.queryParams.putAll(k, v);
              }
            });

    return groupedParams;
  }

  private static Optional<QueryBuilder> buildPostFilter(
      Multimap<OccurrenceSearchParameter, String> postFilterParams) {
    if (postFilterParams == null || postFilterParams.isEmpty()) {
      return Optional.empty();
    }

    BoolQueryBuilder bool = QueryBuilders.boolQuery();
    bool.filter()
        .addAll(
            postFilterParams.asMap().entrySet().stream()
                .flatMap(
                    e ->
                        buildTermQuery(
                            e.getValue(), e.getKey(), SEARCH_TO_ES_MAPPING.get(e.getKey()))
                            .stream())
                .collect(Collectors.toList()));

    return Optional.of(bool);
  }

  private static Optional<List<AggregationBuilder>> buildAggs(
      OccurrenceSearchRequest searchRequest,
      Multimap<OccurrenceSearchParameter, String> postFilterParams,
      boolean facetsEnabled) {
    if (!facetsEnabled
        || searchRequest.getFacets() == null
        || searchRequest.getFacets().isEmpty()) {
      return Optional.empty();
    }

    if (searchRequest.isMultiSelectFacets()
        && postFilterParams != null
        && !postFilterParams.isEmpty()) {
      return Optional.of(buildFacetsMultiselect(searchRequest, postFilterParams));
    }

    return Optional.of(buildFacets(searchRequest));
  }

  private static List<AggregationBuilder> buildFacetsMultiselect(
      OccurrenceSearchRequest searchRequest,
      Multimap<OccurrenceSearchParameter, String> postFilterParams) {

    if (searchRequest.getFacets().size() == 1) {
      // same case as normal facets
      return buildFacets(searchRequest);
    }

    return searchRequest.getFacets().stream()
        .filter(p -> SEARCH_TO_ES_MAPPING.get(p) != null)
        .map(
            facetParam -> {

              // build filter aggs
              BoolQueryBuilder bool = QueryBuilders.boolQuery();
              bool.filter()
                  .addAll(
                      postFilterParams.asMap().entrySet().stream()
                          .filter(entry -> entry.getKey() != facetParam)
                          .flatMap(
                              e ->
                                  buildTermQuery(
                                      e.getValue(),
                                      e.getKey(),
                                      SEARCH_TO_ES_MAPPING.get(e.getKey()))
                                      .stream())
                          .collect(Collectors.toList()));

              // add filter to the aggs
              OccurrenceEsField esField = SEARCH_TO_ES_MAPPING.get(facetParam);
              FilterAggregationBuilder filterAggs =
                  AggregationBuilders.filter(esField.getFieldName(), bool);

              // build terms aggs and add it to the filter aggs
              TermsAggregationBuilder termsAggs =
                  buildTermsAggs(
                      "filtered_" + esField.getFieldName(),
                      esField,
                      extractFacetOffset(searchRequest, facetParam),
                      extractFacetLimit(searchRequest, facetParam),
                      searchRequest.getFacetMinCount());
              filterAggs.subAggregation(termsAggs);

              return filterAggs;
            })
        .collect(Collectors.toList());
  }

  private static List<AggregationBuilder> buildFacets(OccurrenceSearchRequest searchRequest) {
    return searchRequest.getFacets().stream()
        .filter(p -> SEARCH_TO_ES_MAPPING.get(p) != null)
        .map(
            facetParam -> {
              OccurrenceEsField esField = SEARCH_TO_ES_MAPPING.get(facetParam);
              return buildTermsAggs(
                  esField.getFieldName(),
                  esField,
                  extractFacetOffset(searchRequest, facetParam),
                  extractFacetLimit(searchRequest, facetParam),
                  searchRequest.getFacetMinCount());
            })
        .collect(Collectors.toList());
  }

  private static TermsAggregationBuilder buildTermsAggs(
      String aggsName,
      OccurrenceEsField esField,
      int facetOffset,
      int facetLimit,
      Integer minCount) {
    // build aggs for the field
    TermsAggregationBuilder termsAggsBuilder =
        AggregationBuilders.terms(aggsName).field(esField.getFieldName());

    // min count
    Optional.ofNullable(minCount).ifPresent(termsAggsBuilder::minDocCount);

    // aggs size
    termsAggsBuilder.size(calculateAggsSize(esField, facetOffset, facetLimit));

    return termsAggsBuilder;
  }

  private static int calculateAggsSize(OccurrenceEsField esField, int facetOffset, int facetLimit) {
    int maxCardinality = CARDINALITIES.getOrDefault(esField, Integer.MAX_VALUE);

    // offset cannot be greater than the max cardinality
    if (facetOffset >= maxCardinality) {
      throw new IllegalArgumentException(
          "facet paging for "
              + esField.getFieldName()
              + " exceeds the cardinality of the field: "
              + CARDINALITIES.get(esField));
    }

    // the limit is bounded by the max cardinality of the field
    int limit = Math.min(facetOffset + facetLimit, maxCardinality);

    // we set a maximum limit for performance reasons
    if (limit > MAX_SIZE_TERMS_AGGS) {
      throw new IllegalArgumentException(
          "Facets paging is only supported up to " + MAX_SIZE_TERMS_AGGS + " elements");
    }
    return limit;
  }

  private static List<QueryBuilder> buildTermQuery(
      Collection<String> values, OccurrenceSearchParameter param, OccurrenceEsField esField) {
    List<QueryBuilder> queries = new ArrayList<>();

    BiFunction<String, OccurrenceSearchParameter, String> parser =
        (v, p) -> Enum.class.isAssignableFrom(p.type()) ? v.toUpperCase() : v;

    // collect queries for each value
    List<String> parsedValues = new ArrayList<>();
    for (String value : values) {
      if (isRange(value)) {
        queries.add(buildRangeQuery(esField, value));
        continue;
      }

      parsedValues.add(parser.apply(value, param));
    }

    if (parsedValues.size() == 1) {
      // single term
      queries.add(QueryBuilders.termQuery(esField.getFieldName(), parsedValues.get(0)));
    } else if (parsedValues.size() > 1) {
      // multi term query
      queries.add(QueryBuilders.termsQuery(esField.getFieldName(), parsedValues));
    }

    return queries;
  }

  private static RangeQueryBuilder buildRangeQuery(OccurrenceEsField esField, String value) {
    String[] values = value.split(RANGE_SEPARATOR);
    return QueryBuilders.rangeQuery(esField.getFieldName()).gte(values[0]).lte(values[1]);
  }

  private static List<Coordinate> asCollectionOfCoordinates(
      com.vividsolutions.jts.geom.Coordinate[] coordinates) {
    return Arrays.stream(coordinates)
        .map(coord -> new Coordinate(coord.x, coord.y))
        .collect(Collectors.toList());
  }

  private static GeoShapeQueryBuilder buildGeoShapeQuery(String wkt) {
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
                          asCollectionOfCoordinates(polygon.getExteriorRing().getCoordinates())));
          for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            polygonBuilder.hole(
                new LineStringBuilder(
                    new CoordinatesBuilder()
                        .coordinates(
                            asCollectionOfCoordinates(
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
      shapeBuilder = new LineStringBuilder(asCollectionOfCoordinates(geometry.getCoordinates()));
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
      return QueryBuilders.geoShapeQuery(COORDINATE_SHAPE.getFieldName(), shapeBuilder)
          .relation(ShapeRelation.WITHIN);
    } catch (IOException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }

  @VisibleForTesting
  static class GroupedParams {
    Multimap<OccurrenceSearchParameter, String> postFilterParams;
    Multimap<OccurrenceSearchParameter, String> queryParams;
  }
}
