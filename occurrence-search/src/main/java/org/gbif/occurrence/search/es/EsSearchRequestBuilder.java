package org.gbif.occurrence.search.es;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;
import com.vividsolutions.jts.geom.*;
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
import java.util.*;
import java.util.function.Function;

import static org.gbif.api.util.SearchTypeValidator.isRange;
import static org.gbif.occurrence.search.es.EsQueryUtils.*;

class EsSearchRequestBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(EsSearchRequestBuilder.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectWriter WRITER = MAPPER.writer();

  // TODO: sorting!!

  private EsSearchRequestBuilder() {}

  public static HttpEntity buildHeatmapRequestBody(OccurrenceSearchRequest searchRequest) {
    return createEntity(buildQuery(searchRequest, true));
  }

  static HttpEntity buildRequestBody(OccurrenceSearchRequest searchRequest) {
    // Preconditions.checkArgument(searchRequest.getOffset() <= maxOffset -
    // searchRequest.getLimit(),
    //  "maximum offset allowed is %s", this.maxOffset);

    return createEntity(buildQuery(searchRequest, false));
  }

  @VisibleForTesting
  static ObjectNode buildQuery(OccurrenceSearchRequest request, boolean filtered) {
    // get query params
    Multimap<OccurrenceSearchParameter, String> params = request.getParameters();
    if (params == null || params.isEmpty()) {
      return MAPPER.createObjectNode();
    }

    // create bool node
    ObjectNode bool = MAPPER.createObjectNode();

    // geometry
    if (params.containsKey(OccurrenceSearchParameter.GEOMETRY)) {
      ArrayNode filterNode = MAPPER.createArrayNode();
      bool.put(FILTER, filterNode);
      params
          .get(OccurrenceSearchParameter.GEOMETRY)
          .forEach(wkt -> filterNode.add(buildGeoShapeQuery(wkt)));
    }

    // term queries
    List<ObjectNode> termQueries = buildTermQueries(params);
    if (!termQueries.isEmpty()) {
      // bool must
      ArrayNode contextNode = MAPPER.createArrayNode();
      bool.put(filtered ? FILTER : MUST, contextNode);
      termQueries.forEach(contextNode::add);
    }

    // build request
    ObjectNode query = MAPPER.createObjectNode();
    query.put(BOOL, bool);
    ObjectNode requestBody = MAPPER.createObjectNode();
    requestBody.put(QUERY, query);

    LOG.debug("ES query: {}", requestBody);

    return requestBody;
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
        coordinate -> {
          ArrayNode node = MAPPER.createArrayNode();
          node.add(coordinate.x);
          node.add(coordinate.y);
          return node;
        };

    Function<Geometry, ArrayNode> geometryToArray =
        geom -> {
          ArrayNode node = MAPPER.createArrayNode();
          Arrays.stream(geom.getCoordinates()).forEach(c -> node.add(coordinateToArray.apply(c)));
          return node;
        };

    Function<Polygon, ArrayNode> polygonToArray =
        polygon -> {
          ArrayNode nodes = MAPPER.createArrayNode();
          nodes.add(geometryToArray.apply(polygon.getExteriorRing()));
          for (int j = 0; j < polygon.getNumInteriorRing(); j++) {
            nodes.add(geometryToArray.apply(polygon.getInteriorRingN(j)));
          }
          return nodes;
        };

    // create coordinates node
    ArrayNode coordinates = MAPPER.createArrayNode();
    if (geometry instanceof Point) {
      coordinates = coordinateToArray.apply(geometry.getCoordinate());
    } else if (geometry instanceof Polygon) {
      polygonToArray.apply((Polygon) geometry).forEach(coordinates::add);
    } else if (geometry instanceof MultiPolygon) {
      // iterate thru the polygons
      for (int i = 0; i < geometry.getNumGeometries(); i++) {
        ArrayNode polygonList = MAPPER.createArrayNode();
        polygonToArray.apply((Polygon) geometry.getGeometryN(i)).forEach(polygonList::add);
        coordinates.add(polygonList);
      }
    } else {
      geometryToArray.apply(geometry).forEach(coordinates::add);
    }

    ObjectNode shapeNode = MAPPER.createObjectNode();
    shapeNode.put(TYPE, type);
    shapeNode.put(COORDINATES, coordinates);

    ObjectNode coordinateNote = MAPPER.createObjectNode();
    coordinateNote.put(SHAPE, shapeNode);
    coordinateNote.put(RELATION, WITHIN);
    ObjectNode geoShapeNode = MAPPER.createObjectNode();
    geoShapeNode.put(OccurrenceEsField.COORDINATE_SHAPE.getFieldName(), coordinateNote);
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

  private static List<ObjectNode> buildTermQueries(
      Multimap<OccurrenceSearchParameter, String> params) {
    // must term fields
    List<ObjectNode> termQueries = new ArrayList<>();
    for (OccurrenceSearchParameter param : params.keySet()) {
      OccurrenceEsField esField = QUERY_FIELD_MAPPING.get(param);
      if (esField == null) {
        continue;
      }

      List<String> termValues = new ArrayList<>();
      for (String value : params.get(param)) {
        if (isRange(value)) {
          termQueries.add(buildRangeQuery(esField, value));
        } else if (param.type() != Date.class) {
          if (Enum.class.isAssignableFrom(param.type())) { // enums are capitalized
            value = value.toUpperCase();
          }
          termValues.add(value);
        }
      }
      createTermQuery(esField, termValues).ifPresent(termQueries::add);
    }
    return termQueries;
  }

  private static Optional<ObjectNode> createTermQuery(
      OccurrenceEsField esField, List<String> parsedValues) {
    if (!parsedValues.isEmpty()) {
      if (parsedValues.size() > 1) {
        // multi term query
        return Optional.of(createMultitermQuery(esField, parsedValues));
      } else {
        // single term
        return Optional.of(createSingleTermQuery(esField, parsedValues.get(0)));
      }
    }
    return Optional.empty();
  }

  private static ObjectNode createSingleTermQuery(OccurrenceEsField esField, String parsedValue) {
    ObjectNode termQuery = MAPPER.createObjectNode();
    termQuery.put(esField.getFieldName(), parsedValue);
    ObjectNode term = MAPPER.createObjectNode();
    term.put(TERM, termQuery);
    return term;
  }

  private static ObjectNode createMultitermQuery(
      OccurrenceEsField esField, List<String> parsedValues) {
    ObjectNode multitermQuery = MAPPER.createObjectNode();
    ArrayNode termsArray = MAPPER.createArrayNode();
    parsedValues.forEach(termsArray::add);
    multitermQuery.put(esField.getFieldName(), termsArray);
    ObjectNode terms = MAPPER.createObjectNode();
    terms.put(TERMS, multitermQuery);
    return terms;
  }

  private static HttpEntity createEntity(ObjectNode json) {
    try {
      return new NStringEntity(WRITER.writeValueAsString(json));
    } catch (IOException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }
}
