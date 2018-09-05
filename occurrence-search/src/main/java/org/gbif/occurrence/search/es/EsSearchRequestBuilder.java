package org.gbif.occurrence.search.es;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.*;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.gbif.api.util.SearchTypeValidator.isRange;
import static org.gbif.occurrence.search.es.EsQueryUtils.SEARCH_TO_ES_MAPPING;
import static org.gbif.occurrence.search.es.EsQueryUtils.RANGE_SEPARATOR;

public class EsSearchRequestBuilder {

  // TODO: sorting!!

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

    // group params
    GroupedParams groupedParams = groupParameters(searchRequest);

    // add query
    buildQuery(groupedParams.queryParams).ifPresent(searchSourceBuilder::query);

    // add aggs
    buildAggs(searchRequest, groupedParams.postFilterParams, facetsEnabled)
        .ifPresent(aggsList -> aggsList.forEach(searchSourceBuilder::aggregation));

    // post-filter
    buildPostFilter(groupedParams.postFilterParams).ifPresent(searchSourceBuilder::postFilter);

    return esRequest;
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
    postFilterParams
        .asMap()
        .forEach(
            (k, v) ->
                buildTermQuery(v, k, SEARCH_TO_ES_MAPPING.get(k))
                    .forEach(q -> bool.filter().add(q)));

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

    return searchRequest
        .getFacets()
        .stream()
        .filter(p -> SEARCH_TO_ES_MAPPING.get(p) != null)
        .map(
            facetParam -> {

              // build filter aggs
              BoolQueryBuilder bool = QueryBuilders.boolQuery();
              postFilterParams
                  .asMap()
                  .entrySet()
                  .stream()
                  .filter(entry -> entry.getKey() != facetParam)
                  .forEach(
                      e ->
                          buildTermQuery(
                                  e.getValue(), e.getKey(), SEARCH_TO_ES_MAPPING.get(e.getKey()))
                              .forEach(q -> bool.filter().add(q)));

              // add filter to the aggs
              OccurrenceEsField esField = SEARCH_TO_ES_MAPPING.get(facetParam);
              FilterAggregationBuilder filterAggs =
                  AggregationBuilders.filter(esField.getFieldName(), bool);

              // build terms aggs and add it to the filter aggs
              TermsAggregationBuilder termsAggs =
                  buildTermsAggs(
                      "filtered_" + esField.getFieldName(),
                      esField,
                      searchRequest.getFacetPage(facetParam),
                      searchRequest.getFacetMinCount());
              filterAggs.subAggregation(termsAggs);

              return filterAggs;
            })
        .collect(Collectors.toList());
  }

  private static List<AggregationBuilder> buildFacets(OccurrenceSearchRequest searchRequest) {
    return searchRequest
        .getFacets()
        .stream()
        .filter(p -> SEARCH_TO_ES_MAPPING.get(p) != null)
        .map(
            facetParam -> {
              OccurrenceEsField esField = SEARCH_TO_ES_MAPPING.get(facetParam);
              return buildTermsAggs(
                  esField.getFieldName(),
                  esField,
                  searchRequest.getFacetPage(facetParam),
                  searchRequest.getFacetMinCount());
            })
        .collect(Collectors.toList());
  }

  private static TermsAggregationBuilder buildTermsAggs(
      String aggsName, OccurrenceEsField esField, Pageable facetPage, Integer minCount) {
    TermsAggregationBuilder termsAggsBuilder =
        AggregationBuilders.terms(aggsName).field(esField.getFieldName());

    Optional.ofNullable(facetPage).ifPresent(p -> termsAggsBuilder.size(p.getLimit()));
    Optional.ofNullable(minCount).ifPresent(termsAggsBuilder::minDocCount);

    // TODO: offset not supported in ES. Implement workaround

    return termsAggsBuilder;
  }

  public static Optional<QueryBuilder> buildQuery(
      Multimap<OccurrenceSearchParameter, String> params) {
    // get query params
    if (params == null || params.isEmpty()) {
      return Optional.empty();
    }

    // create bool node
    BoolQueryBuilder bool = QueryBuilders.boolQuery();

    // adding geometry to bool
    if (params.containsKey(OccurrenceSearchParameter.GEOMETRY)) {
      BoolQueryBuilder shouldGeometry = QueryBuilders.boolQuery();
      params
          .get(OccurrenceSearchParameter.GEOMETRY)
          .forEach(wkt -> shouldGeometry.should().add(buildGeoShapeQuery(wkt)));
      bool.filter().add(shouldGeometry);
    }

    // adding term queries to bool
    params
        .asMap()
        .entrySet()
        .stream()
        .filter(e -> Objects.nonNull(SEARCH_TO_ES_MAPPING.get(e.getKey())))
        .forEach(
            e ->
                buildTermQuery(e.getValue(), e.getKey(), SEARCH_TO_ES_MAPPING.get(e.getKey()))
                    .forEach(q -> bool.filter().add(q)));

    return Optional.of(bool);
  }

  private static List<QueryBuilder> buildTermQuery(
      Collection<String> values, OccurrenceSearchParameter param, OccurrenceEsField esField) {
    List<QueryBuilder> queries = new ArrayList<>();

    // collect queries for each value
    List<String> parsedValues = new ArrayList<>();
    for (String value : values) {
      if (isRange(value)) {
        queries.add(buildRangeQuery(esField, value));
      } else if (param.type() != Date.class) {
        if (Enum.class.isAssignableFrom(param.type())) { // enums are capitalized
          value = value.toUpperCase();
        }
        parsedValues.add(value);
      }
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
    return QueryBuilders.rangeQuery(esField.getFieldName())
        .gte(Double.valueOf(values[0]))
        .lte(Double.valueOf(values[1]));
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
              ShapeBuilders.newPolygon(Arrays.asList(polygon.getExteriorRing().getCoordinates()));
          for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
            polygonBuilder.hole(
                ShapeBuilders.newLineString(
                    Arrays.asList(polygon.getInteriorRingN(i).getCoordinates())));
          }
          return polygonBuilder;
        };

    String type =
        "LinearRing".equals(geometry.getGeometryType())
            ? "LINESTRING"
            : geometry.getGeometryType().toUpperCase();

    ShapeBuilder shapeBuilder = null;
    if (("POINT").equals(type)) {
      shapeBuilder = ShapeBuilders.newPoint(geometry.getCoordinate());
    } else if ("LINESTRING".equals(type)) {
      shapeBuilder = ShapeBuilders.newLineString(Arrays.asList(geometry.getCoordinates()));
    } else if ("POLYGON".equals(type)) {
      shapeBuilder = polygonToBuilder.apply((Polygon) geometry);
    } else if ("MULTIPOLYGON".equals(type)) {
      // multipolygon
      MultiPolygonBuilder multiPolygonBuilder = ShapeBuilders.newMultiPolygon();
      for (int i = 0; i < geometry.getNumGeometries(); i++) {
        multiPolygonBuilder.polygon(polygonToBuilder.apply((Polygon) geometry.getGeometryN(i)));
      }
      shapeBuilder = multiPolygonBuilder;
    } else {
      throw new IllegalArgumentException(type + " shape is not supported");
    }

    try {
      return QueryBuilders.geoShapeQuery(
              OccurrenceEsField.COORDINATE_SHAPE.getFieldName(), shapeBuilder)
          .relation(ShapeRelation.WITHIN);
    } catch (IOException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }

  static class GroupedParams {
    Multimap<OccurrenceSearchParameter, String> postFilterParams;
    Multimap<OccurrenceSearchParameter, String> queryParams;
  }
}
