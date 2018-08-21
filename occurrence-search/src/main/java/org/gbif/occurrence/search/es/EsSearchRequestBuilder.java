package org.gbif.occurrence.search.es;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.http.HttpEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

import static org.gbif.api.util.SearchTypeValidator.isRange;
import static org.gbif.occurrence.search.es.EsQueryUtils.*;

class EsSearchRequestBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(EsSearchRequestBuilder.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectWriter WRITER = MAPPER.writer();

  // TODO: sorting!!

  private EsSearchRequestBuilder() {}

  static HttpEntity buildRequestBody(OccurrenceSearchRequest searchRequest) {
    // Preconditions.checkArgument(searchRequest.getOffset() <= maxOffset -
    // searchRequest.getLimit(),
    //  "maximum offset allowed is %s", this.maxOffset);

    // create body
    ObjectNode request = MAPPER.createObjectNode();
    request.put(QUERY, buildQuery(searchRequest));

    LOG.debug("ES query: {}", request);

    return createEntity(request);
  }

  @VisibleForTesting
  static ObjectNode buildQuery(OccurrenceSearchRequest request) {
    // create root nodes
    ObjectNode query = MAPPER.createObjectNode();
    ObjectNode bool = MAPPER.createObjectNode();
    query.put(BOOL, bool);

    // get query params
    Multimap<OccurrenceSearchParameter, String> params = request.getParameters();
    if (params == null || params.isEmpty()) {
      return query;
    }

    // rest of the fields
    List<ObjectNode> mustMatches = new ArrayList<>();
    for (OccurrenceSearchParameter param : params.keySet()) {
      OccurrenceEsField esField = QUERY_FIELD_MAPPING.get(param);
      if (esField != null) {
        for (String value : params.get(param)) {
          if (isRange(value)) {
            mustMatches.add(buildRangeQuery(esField, value));
          } else if (param.type() != Date.class) {
            // TODO: check parsing when implementing full text search queries
            // String parsedValue = QueryUtils.parseQueryValue(value);
            if (Enum.class.isAssignableFrom(param.type())) { // enums are capitalized
              value = value.toUpperCase();
            }
            mustMatches.add(createMatch(esField, value));
          }
        }

        // build the must query
        if (!mustMatches.isEmpty()) {
          // bool must
          ArrayNode mustNode = MAPPER.createArrayNode();
          bool.put(MUST, mustNode);
          mustMatches.forEach(mustNode::add);
        }
      }
    }
    // TODO: dates, ranges and geo points
    // addLocationQuery(params, solrQuery, isFacetedSearch);
    // addDateQuery(params, OccurrenceSearchParameter.EVENT_DATE, OccurrenceEsField.EVENT_DATE,
    // solrQuery,
    //             isFacetedSearch);
    // addDateQuery(params, OccurrenceSearchParameter.LAST_INTERPRETED,
    // OccurrenceEsField.LAST_INTERPRETED, solrQuery,
    //             isFacetedSearch);

    return query;
  }

  @VisibleForTesting
  static ObjectNode buildGeoShapeQuery(String wkt) {
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

    Function<Coordinate, ArrayNode> coordinateToArray =
        c -> {
          ArrayNode node = MAPPER.createArrayNode();
          node.add(c.x);
          node.add(c.y);
          return node;
        };

    Function<Geometry, ArrayNode> geometryToArray =
        g -> {
          ArrayNode node = MAPPER.createArrayNode();
          Arrays.stream(g.getCoordinates()).forEach(c -> node.add(coordinateToArray.apply(c)));
          return node;
        };

    Function<Polygon, List<ArrayNode>> polygonToArray =
        p -> {
          List<ArrayNode> nodes = new ArrayList<>();
          nodes.add(geometryToArray.apply(p.getExteriorRing()));
          for (int j = 0; j < p.getNumInteriorRing(); j++) {
            nodes.add(geometryToArray.apply(p.getInteriorRingN(j)));
          }
          return nodes;
        };

    ArrayNode coordinates = MAPPER.createArrayNode();
    for (int i = 0; i < geometry.getNumGeometries(); i++) {
      Geometry geom = geometry.getGeometryN(i);

      if ("POLYGON".equals(type)) {
        polygonToArray.apply((Polygon) geom).forEach(n -> coordinates.add(n));
      } else if ("MULTIPOLYGON".equals(type)) {
        ArrayNode polygonArray = MAPPER.createArrayNode();
        polygonToArray.apply((Polygon) geom).forEach(n -> polygonArray.add(n));
        coordinates.add(polygonArray);
      } else {
        coordinates.add(geometryToArray.apply(geom));
      }
    }

    ObjectNode shapeNode = MAPPER.createObjectNode();
    shapeNode.put(TYPE, type);
    shapeNode.put(COORDINATES, coordinates);

    ObjectNode coordinateNote = MAPPER.createObjectNode();
    coordinateNote.put(SHAPE, shapeNode);

    ObjectNode geoShapeNode = MAPPER.createObjectNode();
    geoShapeNode.put(OccurrenceEsField.COORDINATE.getFieldName(), coordinateNote);

    ObjectNode root = MAPPER.createObjectNode();
    root.put(GEO_SHAPE, geoShapeNode);

    return root;
  }

  private static ObjectNode buildRangeQuery(OccurrenceEsField esField, String value) {
    ObjectNode root = MAPPER.createObjectNode();

    String[] values = value.split(RANGE_SEPARATOR);
    if (values.length < 2) {
      return root;
    }

    ObjectNode range = MAPPER.createObjectNode();
    range.put(GTE, Double.valueOf(values[0]));
    range.put(LTE, Double.valueOf(values[1]));

    ObjectNode field = MAPPER.createObjectNode();
    field.put(esField.getFieldName(), range);

    root.put(RANGE, field);

    return root;
  }

  private static ObjectNode createMatch(OccurrenceEsField esField, String parsedValue) {
    ObjectNode matchQuery = MAPPER.createObjectNode();
    matchQuery.put(esField.getFieldName(), parsedValue);
    ObjectNode match = MAPPER.createObjectNode();
    match.put(MATCH, matchQuery);
    return match;
  }

  private static HttpEntity createEntity(ObjectNode json) {
    try {
      return new NStringEntity(WRITER.writeValueAsString(json));
    } catch (IOException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }
}
