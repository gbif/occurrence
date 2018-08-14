package org.gbif.occurrence.search.es;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import org.apache.http.HttpEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.lucene.search.BooleanClause;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.gbif.api.util.SearchTypeValidator.isRange;

public class EsSearchRequestBuilder {

  // cords constants
  private static final double MIN_DIFF = 0.000001;
  private static final double MIN_LON = -180;
  private static final double MAX_LON = 180;
  private static final double MIN_LAT = -90;
  private static final double MAX_LAT = 90;

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
    request.put("query", buildQuery(searchRequest));

    return createEntity(request);
  }

  // TODO: check parsing when implementing full text search queries
  @VisibleForTesting
  static ObjectNode buildQuery(OccurrenceSearchRequest request) {
    // create root nodes
    ObjectNode query = MAPPER.createObjectNode();
    ObjectNode bool = MAPPER.createObjectNode();
    query.put("bool", bool);

    // get query params
    Multimap<OccurrenceSearchParameter, String> params = request.getParameters();
    if (params == null || params.isEmpty()) {
      return query;
    }

    // coordinates query
    buildCoordinatesQuery(params).ifPresent(q -> bool.put("filter", q));

    // rest of the fields
    List<ObjectNode> mustMatches = new ArrayList<>();
    for (OccurrenceSearchParameter param : params.keySet()) {
      OccurrenceEsField esField = QUERY_FIELD_MAPPING.get(param);
      if (esField != null) {
        for (String value : params.get(param)) {
          if (param == OccurrenceSearchParameter.TAXON_KEY) {
            bool.put("should", buildTaxonKeyQuery(value));
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
          bool.put("must", mustNode);
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
            .map(values -> values.iterator().next());

    Optional<String> lonParam =
        Optional.ofNullable(params.get(OccurrenceSearchParameter.DECIMAL_LONGITUDE))
            .map(values -> values.iterator().next());

    BiFunction<Double, Double, ObjectNode> pointNode =
        (lat, lon) -> {
          ObjectNode location = MAPPER.createObjectNode();
          location.put("lat", lat);
          location.put("lon", lon);
          return location;
        };

    // if it's a single point we perform a geo distance query
    if (latParam.isPresent()
        && !isRange(latParam.get())
        && lonParam.isPresent()
        && !isRange(lonParam.get())) {
      ObjectNode geoDistance = MAPPER.createObjectNode();
      geoDistance.put("distance", "1m");
      geoDistance.put(
          "location",
          pointNode.apply(
              latParam.map(Double::valueOf).get(), lonParam.map(Double::valueOf).get()));
      ObjectNode filter = MAPPER.createObjectNode();
      filter.put("geo_distance", geoDistance);
      return Optional.of(filter);
    }

    // converts string to a range
    Function<String, CoordsRange> rangeConverter =
        coord -> {
          String[] values = coord.split(",");
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
    geoBoundingBox.put("location", location);
    ObjectNode filter = MAPPER.createObjectNode();
    filter.put("geo_bounding_box", geoBoundingBox);

    // top left
    location.put("top_left", pointNode.apply(latRange.max, lonRange.min));
    // bottom right
    location.put("bottom_right", pointNode.apply(latRange.min, lonRange.max));

    // clean params
    params.removeAll(OccurrenceSearchParameter.DECIMAL_LATITUDE);
    params.removeAll(OccurrenceSearchParameter.DECIMAL_LONGITUDE);

    return Optional.of(filter);
  }

  private static ObjectNode createMatch(OccurrenceEsField esField, String parsedValue) {
    ObjectNode matchQuery = MAPPER.createObjectNode();
    matchQuery.put(esField.getFieldName(), parsedValue);
    ObjectNode match = MAPPER.createObjectNode();
    match.put("match", matchQuery);
    return match;
  }

  private static ArrayNode buildTaxonKeyQuery(String taxonKey) {
    ArrayNode shouldNode = MAPPER.createArrayNode();

    OccurrenceEsField.TAXON_KEYS_LIST.forEach(
        key -> {
          ObjectNode termQuery = MAPPER.createObjectNode();
          termQuery.put(key.getFieldName(), taxonKey);
          ObjectNode term = MAPPER.createObjectNode();
          term.put("term", termQuery);
          shouldNode.add(term);
        });

    return shouldNode;
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

  // This is a placeholder to map from the JSON definition ID to the query field
  private static final ImmutableMap<OccurrenceSearchParameter, OccurrenceEsField>
      QUERY_FIELD_MAPPING =
          ImmutableMap.<OccurrenceSearchParameter, OccurrenceEsField>builder()
              .put(OccurrenceSearchParameter.DECIMAL_LATITUDE, OccurrenceEsField.LATITUDE)
              .put(OccurrenceSearchParameter.DECIMAL_LONGITUDE, OccurrenceEsField.LONGITUDE)
              .put(OccurrenceSearchParameter.YEAR, OccurrenceEsField.YEAR)
              .put(OccurrenceSearchParameter.MONTH, OccurrenceEsField.MONTH)
              .put(OccurrenceSearchParameter.CATALOG_NUMBER, OccurrenceEsField.CATALOG_NUMBER)
              .put(OccurrenceSearchParameter.RECORDED_BY, OccurrenceEsField.RECORDED_BY)
              .put(OccurrenceSearchParameter.RECORD_NUMBER, OccurrenceEsField.RECORD_NUMBER)
              .put(OccurrenceSearchParameter.COLLECTION_CODE, OccurrenceEsField.COLLECTION_CODE)
              .put(OccurrenceSearchParameter.INSTITUTION_CODE, OccurrenceEsField.INSTITUTION_CODE)
              .put(OccurrenceSearchParameter.DEPTH, OccurrenceEsField.DEPTH)
              .put(OccurrenceSearchParameter.ELEVATION, OccurrenceEsField.ELEVATION)
              .put(OccurrenceSearchParameter.BASIS_OF_RECORD, OccurrenceEsField.BASIS_OF_RECORD)
              .put(OccurrenceSearchParameter.DATASET_KEY, OccurrenceEsField.DATASET_KEY)
              .put(OccurrenceSearchParameter.HAS_GEOSPATIAL_ISSUE, OccurrenceEsField.SPATIAL_ISSUES)
              .put(OccurrenceSearchParameter.HAS_COORDINATE, OccurrenceEsField.HAS_COORDINATE)
              .put(OccurrenceSearchParameter.EVENT_DATE, OccurrenceEsField.EVENT_DATE)
              .put(OccurrenceSearchParameter.LAST_INTERPRETED, OccurrenceEsField.LAST_INTERPRETED)
              .put(OccurrenceSearchParameter.COUNTRY, OccurrenceEsField.COUNTRY_CODE)
              .put(
                  OccurrenceSearchParameter.PUBLISHING_COUNTRY,
                  OccurrenceEsField.PUBLISHING_COUNTRY)
              .put(OccurrenceSearchParameter.CONTINENT, OccurrenceEsField.CONTINENT)
              .put(OccurrenceSearchParameter.TAXON_KEY, OccurrenceEsField.TAXON_KEY)
              .put(OccurrenceSearchParameter.KINGDOM_KEY, OccurrenceEsField.KINGDOM_KEY)
              .put(OccurrenceSearchParameter.PHYLUM_KEY, OccurrenceEsField.PHYLUM_KEY)
              .put(OccurrenceSearchParameter.CLASS_KEY, OccurrenceEsField.CLASS_KEY)
              .put(OccurrenceSearchParameter.ORDER_KEY, OccurrenceEsField.ORDER_KEY)
              .put(OccurrenceSearchParameter.FAMILY_KEY, OccurrenceEsField.FAMILY_KEY)
              .put(OccurrenceSearchParameter.GENUS_KEY, OccurrenceEsField.GENUS_KEY)
              .put(OccurrenceSearchParameter.SUBGENUS_KEY, OccurrenceEsField.SUBGENUS_KEY)
              .put(OccurrenceSearchParameter.SPECIES_KEY, OccurrenceEsField.SPECIES_KEY)
              .put(OccurrenceSearchParameter.SCIENTIFIC_NAME, OccurrenceEsField.SCIENTIFIC_NAME)
              .put(OccurrenceSearchParameter.TYPE_STATUS, OccurrenceEsField.TYPE_STATUS)
              .put(OccurrenceSearchParameter.MEDIA_TYPE, OccurrenceEsField.MEDIA_TYPE)
              .put(OccurrenceSearchParameter.ISSUE, OccurrenceEsField.ISSUE)
              .put(OccurrenceSearchParameter.OCCURRENCE_ID, OccurrenceEsField.OCCURRENCE_ID)
              .put(
                  OccurrenceSearchParameter.ESTABLISHMENT_MEANS,
                  OccurrenceEsField.ESTABLISHMENT_MEANS)
              .put(OccurrenceSearchParameter.REPATRIATED, OccurrenceEsField.REPATRIATED)
              .put(OccurrenceSearchParameter.LOCALITY, OccurrenceEsField.LOCALITY)
              .put(OccurrenceSearchParameter.STATE_PROVINCE, OccurrenceEsField.STATE_PROVINCE)
              .put(OccurrenceSearchParameter.WATER_BODY, OccurrenceEsField.WATER_BODY)
              .put(OccurrenceSearchParameter.LICENSE, OccurrenceEsField.LICENSE)
              .put(OccurrenceSearchParameter.PROTOCOL, OccurrenceEsField.PROTOCOL)
              .put(OccurrenceSearchParameter.ORGANISM_ID, OccurrenceEsField.ORGANISM_ID)
              .put(
                  OccurrenceSearchParameter.PUBLISHING_ORG,
                  OccurrenceEsField.PUBLISHING_ORGANIZATION_KEY)
              .put(OccurrenceSearchParameter.CRAWL_ID, OccurrenceEsField.CRAWL_ID)
              .put(OccurrenceSearchParameter.INSTALLATION_KEY, OccurrenceEsField.INSTALLATION_KEY)
              .put(OccurrenceSearchParameter.NETWORK_KEY, OccurrenceEsField.NETWORK_KEY)
              .put(OccurrenceSearchParameter.EVENT_ID, OccurrenceEsField.EVENT_ID)
              .put(OccurrenceSearchParameter.PARENT_EVENT_ID, OccurrenceEsField.PARENT_EVENT_ID)
              .put(OccurrenceSearchParameter.SAMPLING_PROTOCOL, OccurrenceEsField.SAMPLING_PROTOCOL)
              .build();
}
