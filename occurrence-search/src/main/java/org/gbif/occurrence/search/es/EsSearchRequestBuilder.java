package org.gbif.occurrence.search.es;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Multimap;
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
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.gbif.api.util.SearchTypeValidator.isRange;
import static org.gbif.occurrence.search.es.EsQueryUtils.*;

public class EsSearchRequestBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(EsSearchRequestBuilder.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectWriter WRITER = MAPPER.writer();

  // TODO: sorting!!

  private EsSearchRequestBuilder() {}

  public static HttpEntity buildRequestBody(OccurrenceSearchRequest searchRequest) {
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

    // coordinates query
    buildCoordinatesQuery(params).ifPresent(q -> bool.put(FILTER, q));

    // rest of the fields
    List<ObjectNode> mustMatches = new ArrayList<>();
    for (OccurrenceSearchParameter param : params.keySet()) {
      OccurrenceEsField esField = QUERY_FIELD_MAPPING.get(param);
      if (esField != null) {
        for (String value : params.get(param)) {
          if (param.type() != Date.class) {
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

  private static Optional<ObjectNode> buildCoordinatesQuery(
      Multimap<OccurrenceSearchParameter, String> params) {
    if (!params.containsKey(OccurrenceSearchParameter.DECIMAL_LATITUDE)
        && !params.containsKey(OccurrenceSearchParameter.DECIMAL_LONGITUDE)) {
      return Optional.empty();
    }

    Optional<String> latParam =
        Optional.ofNullable(params.get(OccurrenceSearchParameter.DECIMAL_LATITUDE))
            .filter(p -> !p.isEmpty())
            .map(values -> values.iterator().next());

    Optional<String> lonParam =
        Optional.ofNullable(params.get(OccurrenceSearchParameter.DECIMAL_LONGITUDE))
            .filter(p -> !p.isEmpty())
            .map(values -> values.iterator().next());

    // clear params
    params.removeAll(OccurrenceSearchParameter.DECIMAL_LATITUDE);
    params.removeAll(OccurrenceSearchParameter.DECIMAL_LONGITUDE);

    BiFunction<Double, Double, ObjectNode> pointNode =
        (lat, lon) -> {
          ObjectNode location = MAPPER.createObjectNode();
          location.put(LAT, lat);
          location.put(LON, lon);
          return location;
        };

    // if it's a single point we perform a geo distance query
    if (latParam.isPresent()
        && !isRange(latParam.get())
        && lonParam.isPresent()
        && !isRange(lonParam.get())) {
      ObjectNode geoDistance = MAPPER.createObjectNode();
      geoDistance.put(DISTANCE, DEFAULT_DISTANCE);
      geoDistance.put(
          LOCATION,
          pointNode.apply(
              latParam.map(Double::valueOf).get(), lonParam.map(Double::valueOf).get()));
      ObjectNode filter = MAPPER.createObjectNode();
      filter.put(GEO_DISTANCE, geoDistance);
      return Optional.of(filter);
    }

    // converts string to a range
    Function<String, CoordsRange> rangeConverter =
        coord -> {
          String[] values = coord.split(RANGE_SEPARATOR);
          double min = Double.valueOf(values[0]) - MIN_DIFF;
          double max =
              (values.length > 1 ? Double.valueOf(values[1]) : Double.valueOf(values[0]))
                  + MIN_DIFF;
          return new CoordsRange(min, max);
        };

    CoordsRange latRange = latParam.map(rangeConverter).orElse(new CoordsRange(MIN_LAT, MAX_LAT));
    CoordsRange lonRange = lonParam.map(rangeConverter).orElse(new CoordsRange(MIN_LON, MAX_LON));

    ObjectNode geoBoundingBox = MAPPER.createObjectNode();
    ObjectNode location = MAPPER.createObjectNode();
    geoBoundingBox.put(LOCATION, location);
    ObjectNode filter = MAPPER.createObjectNode();
    filter.put(GEO_BOUNDING_BOX, geoBoundingBox);

    // top left
    location.put(TOP_LEFT, pointNode.apply(latRange.max, lonRange.min));
    // bottom right
    location.put(BOTTOM_RIGHT, pointNode.apply(latRange.min, lonRange.max));

    return Optional.of(filter);
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

  private static class CoordsRange {
    double min;
    double max;

    CoordsRange(double min, double max) {
      this.min = min;
      this.max = max;
    }
  }
}
