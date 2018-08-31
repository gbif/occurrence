package org.gbif.occurrence.search.es;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.codehaus.jackson.node.ObjectNode;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.*;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;

import static org.gbif.api.util.SearchTypeValidator.isRange;
import static org.gbif.occurrence.search.es.EsQueryUtils.QUERY_FIELD_MAPPING;
import static org.gbif.occurrence.search.es.EsQueryUtils.RANGE_SEPARATOR;

class EsSearchRequestBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(EsSearchRequestBuilder.class);

  // TODO: sorting!!

  private EsSearchRequestBuilder() {}

  static SearchRequest buildSearchRequest(
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
    int offset = (int) Math.min(maxOffset, searchRequest.getOffset());
    searchSourceBuilder.from(offset);

    // add query
    buildQuery(searchRequest).ifPresent(searchSourceBuilder::query);

    // TODO: multifacet con post-filter:
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-post-filter.html??
    // si hay multifacet, esos facet se meten en post-filter

    //    // add aggs
    //    buildAggs(searchRequest, facetsEnabled, offset).ifPresent(n -> request.put(AGGS, n));
    //
    //    LOG.debug("ES query: {}", request);

    return esRequest;
  }

  static Optional<ObjectNode> buildAggs(
      OccurrenceSearchRequest searchRequest, boolean facetsEnabled, long offset) {
    if (!facetsEnabled
        || searchRequest.getFacetPages() == null
        || searchRequest.getFacetPages().isEmpty()) {
      return Optional.empty();
    }

    // TODO: sacar cardinality para el size del terms aggs?? de momento hardcoded
    // TODO: facet paging hacerlo en java para coger a partir del offset que nos llegue??

    // iterate thru facets and then check config

    return null;
  }

  @VisibleForTesting
  static Optional<QueryBuilder> buildQuery(OccurrenceSearchRequest searchRequest) {
    // get query params
    Multimap<OccurrenceSearchParameter, String> params = searchRequest.getParameters();
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
    buildTermQueries(params)
        .ifPresent(termQueries -> termQueries.forEach(q -> bool.filter().add(q)));

    return Optional.of(bool);
  }

  private static GeoShapeQueryBuilder buildGeoShapeQuery(String wkt) {
    Geometry geometry;
    try {
      geometry = new WKTReader().read(wkt);
    } catch (ParseException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }

    String type =
        "LinearRing".equals(geometry.getGeometryType())
            ? "LINESTRING"
            : geometry.getGeometryType().toUpperCase();

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
    }

    if (shapeBuilder == null) {
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

  private static Optional<RangeQueryBuilder> buildRangeQuery(
      OccurrenceEsField esField, String value) {
    String[] values = value.split(RANGE_SEPARATOR);
    if (values.length < 2) {
      return Optional.empty();
    }

    return Optional.of(
        QueryBuilders.rangeQuery(esField.getFieldName())
            .gte(Double.valueOf(values[0]))
            .lte(Double.valueOf(values[1])));
  }

  private static Optional<List<QueryBuilder>> buildTermQueries(
      Multimap<OccurrenceSearchParameter, String> params) {
    if (params == null || params.isEmpty()) {
      return Optional.empty();
    }

    // must term fields
    List<QueryBuilder> termQueries = new ArrayList<>();
    for (OccurrenceSearchParameter param : params.keySet()) {
      OccurrenceEsField esField = QUERY_FIELD_MAPPING.get(param);
      if (esField == null) {
        continue;
      }

      List<String> termValues = new ArrayList<>();
      for (String value : params.get(param)) {
        if (isRange(value)) {
          buildRangeQuery(esField, value).ifPresent(termQueries::add);
        } else if (param.type() != Date.class) {
          if (Enum.class.isAssignableFrom(param.type())) { // enums are capitalized
            value = value.toUpperCase();
          }
          termValues.add(value);
        }
      }
      createTermQuery(esField, termValues).ifPresent(termQueries::add);
    }

    return termQueries.isEmpty() ? Optional.empty() : Optional.of(termQueries);
  }

  private static Optional<QueryBuilder> createTermQuery(
      OccurrenceEsField esField, List<String> parsedValues) {
    if (parsedValues.isEmpty()) {
      return Optional.empty();
    }

    if (parsedValues.size() > 1) {
      // multi term query
      return Optional.of(QueryBuilders.termsQuery(esField.getFieldName(), parsedValues));
    }
    // single term
    return Optional.of(QueryBuilders.termQuery(esField.getFieldName(), parsedValues.get(0)));
  }
}
