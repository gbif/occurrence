package org.gbif.occurrence.search.es;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;
import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.http.HttpEntity;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.function.Function;

import static org.gbif.occurrence.search.es.EsQueryUtils.*;

class EsSearchRequestBuilder extends EsRequestBuilderBase {

  private static final Logger LOG = LoggerFactory.getLogger(EsSearchRequestBuilder.class);

  // TODO: sorting!!

  private EsSearchRequestBuilder() {}

  static HttpEntity buildRequestBody(OccurrenceSearchRequest searchRequest) {
    // Preconditions.checkArgument(searchRequest.getOffset() <= maxOffset -
    // searchRequest.getLimit(),
    //  "maximum offset allowed is %s", this.maxOffset);

    return createEntity(buildQuery(searchRequest));
  }

  @VisibleForTesting
  static ObjectNode buildQuery(OccurrenceSearchRequest request) {
    // get query params
    Multimap<OccurrenceSearchParameter, String> params = request.getParameters();
    if (params == null || params.isEmpty()) {
      return createObjectNode();
    }

    // create bool node
    ObjectNode bool = createObjectNode();

    // adding geometry to bool
    if (params.containsKey(OccurrenceSearchParameter.GEOMETRY)) {
      ArrayNode filterNode = createArrayNode();
      bool.put(FILTER, filterNode);
      params
          .get(OccurrenceSearchParameter.GEOMETRY)
          .forEach(wkt -> filterNode.add(buildGeoShapeQuery(wkt)));
    }

    // adding term queries to bool
    buildTermQueries(params)
        .ifPresent(
            termQueries -> {
              // bool must
              ArrayNode mustNode = createArrayNode();
              bool.put(MUST, mustNode);
              termQueries.forEach(mustNode::add);
            });

    // build request body
    ObjectNode query = createObjectNode();
    query.put(BOOL, bool);
    ObjectNode requestBody = createObjectNode();
    requestBody.put(QUERY, query);

    LOG.debug("ES query: {}", requestBody);

    return requestBody;
  }

  private static ObjectNode buildGeoShapeQuery(String wkt) {
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
          ArrayNode node = createArrayNode();
          node.add(coordinate.x);
          node.add(coordinate.y);
          return node;
        };

    Function<Geometry, ArrayNode> geometryToArray =
        geom -> {
          ArrayNode node = createArrayNode();
          Arrays.stream(geom.getCoordinates()).forEach(c -> node.add(coordinateToArray.apply(c)));
          return node;
        };

    Function<Polygon, ArrayNode> polygonToArray =
        polygon -> {
          ArrayNode nodes = createArrayNode();
          nodes.add(geometryToArray.apply(polygon.getExteriorRing()));
          for (int j = 0; j < polygon.getNumInteriorRing(); j++) {
            nodes.add(geometryToArray.apply(polygon.getInteriorRingN(j)));
          }
          return nodes;
        };

    // create coordinates node
    ArrayNode coordinates = createArrayNode();
    if (geometry instanceof Point) {
      coordinates = coordinateToArray.apply(geometry.getCoordinate());
    } else if (geometry instanceof Polygon) {
      polygonToArray.apply((Polygon) geometry).forEach(coordinates::add);
    } else if (geometry instanceof MultiPolygon) {
      // iterate thru the polygons
      for (int i = 0; i < geometry.getNumGeometries(); i++) {
        ArrayNode polygonList = createArrayNode();
        polygonToArray.apply((Polygon) geometry.getGeometryN(i)).forEach(polygonList::add);
        coordinates.add(polygonList);
      }
    } else {
      geometryToArray.apply(geometry).forEach(coordinates::add);
    }

    ObjectNode shapeNode = createObjectNode();
    shapeNode.put(TYPE, type);
    shapeNode.put(COORDINATES, coordinates);

    ObjectNode coordinateNote = createObjectNode();
    coordinateNote.put(SHAPE, shapeNode);
    coordinateNote.put(RELATION, WITHIN);
    ObjectNode geoShapeNode = createObjectNode();
    geoShapeNode.put(OccurrenceEsField.COORDINATE_SHAPE.getFieldName(), coordinateNote);
    ObjectNode root = createObjectNode();
    root.put(GEO_SHAPE, geoShapeNode);

    return root;
  }
}
