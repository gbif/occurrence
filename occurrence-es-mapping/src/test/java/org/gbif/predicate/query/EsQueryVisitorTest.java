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
package org.gbif.predicate.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.gbif.api.exception.QueryBuildingException;
import org.gbif.api.model.occurrence.geo.DistanceUnit;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.ConjunctionPredicate;
import org.gbif.api.model.predicate.DisjunctionPredicate;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.predicate.GeoDistancePredicate;
import org.gbif.api.model.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.predicate.GreaterThanPredicate;
import org.gbif.api.model.predicate.InPredicate;
import org.gbif.api.model.predicate.IsNotNullPredicate;
import org.gbif.api.model.predicate.IsNullPredicate;
import org.gbif.api.model.predicate.LessThanOrEqualsPredicate;
import org.gbif.api.model.predicate.LessThanPredicate;
import org.gbif.api.model.predicate.LikePredicate;
import org.gbif.api.model.predicate.NotPredicate;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.model.predicate.WithinPredicate;
import org.gbif.api.util.IsoDateInterval;
import org.gbif.search.es.occurrence.OccurrenceEsField;
import org.gbif.search.es.occurrence.OccurrenceEsFieldMapper;
import org.junit.jupiter.api.Test;

/**
 * Test cases for the Elasticsearch query visitor.
 *
 * TODO: It's not clear to me why we have this and https://github.com/gbif/gbif-predicates/blob/dev/es-predicates/src/test/java/org/gbif/predicate/query/EsQueryVisitorTest.java,
 * which is similar but has different casing of the ES parameters.
 */
public class EsQueryVisitorTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final OccurrenceSearchParameter PARAM = OccurrenceSearchParameter.CATALOG_NUMBER;
  private static final OccurrenceSearchParameter PARAM2 =
      OccurrenceSearchParameter.INSTITUTION_CODE;

  private final EsFieldMapper<OccurrenceSearchParameter> occurrenceEsFieldMapper =
      OccurrenceEsField.buildFieldMapper();
  private final EsQueryVisitor<OccurrenceSearchParameter> occurrenceVisitor =
      new OccurrenceEsQueryVisitor(occurrenceEsFieldMapper, "defaultChecklistKey");

  private static void assertJsonEquals(String expectedQuery, String actualQuery) {
    try {
      JsonNode expected = OBJECT_MAPPER.readTree(expectedQuery);
      JsonNode actual = OBJECT_MAPPER.readTree(actualQuery);
      normalizeEsJson(expected);
      normalizeEsJson(actual);
      assertEquals(expected, actual);
    } catch (JsonProcessingException e) {
      fail("Error parsing JSON query", e);
    }
  }

  private static void normalizeEsJson(JsonNode node) {
    if (node == null) {
      return;
    }

    if (node.isObject()) {
      ObjectNode objectNode = (ObjectNode) node;

      // Normalize children first.
      List<String> fieldNames = new ArrayList<>();
      Iterator<String> iterator = objectNode.fieldNames();
      while (iterator.hasNext()) {
        fieldNames.add(iterator.next());
      }
      for (String fieldName : fieldNames) {
        normalizeEsJson(objectNode.get(fieldName));
      }

      // Remove ES default fields that old tests used to serialize.
      JsonNode boost = objectNode.get("boost");
      if (boost != null && boost.isNumber() && boost.asDouble() == 1.0d) {
        objectNode.remove("boost");
      }

      JsonNode adjustPureNegative = objectNode.get("adjust_pure_negative");
      if (adjustPureNegative != null && adjustPureNegative.isBoolean() && adjustPureNegative.asBoolean()) {
        objectNode.remove("adjust_pure_negative");
      }

      // Normalize old range representation:
      // from/to + include_lower/include_upper  ->  gt/gte/lt/lte.
      normalizeRangeBounds(objectNode);
      normalizeGeoDistance(objectNode);
      normalizeGeoShape(objectNode);
      return;
    }

    if (node.isArray()) {
      node.forEach(EsQueryVisitorTest::normalizeEsJson);
    }
  }

  private static void normalizeRangeBounds(ObjectNode rangeNode) {
    boolean hasRangeSyntax =
        rangeNode.has("from")
            || rangeNode.has("to")
            || rangeNode.has("include_lower")
            || rangeNode.has("include_upper")
            || rangeNode.has("gt")
            || rangeNode.has("gte")
            || rangeNode.has("lt")
            || rangeNode.has("lte");
    if (!hasRangeSyntax) {
      return;
    }

    JsonNode from = rangeNode.get("from");
    if (from != null && !from.isNull()) {
      boolean includeLower = !rangeNode.has("include_lower") || rangeNode.path("include_lower").asBoolean();
      rangeNode.set(includeLower ? "gte" : "gt", from);
    }

    JsonNode to = rangeNode.get("to");
    if (to != null && !to.isNull()) {
      boolean includeUpper = !rangeNode.has("include_upper") || rangeNode.path("include_upper").asBoolean();
      rangeNode.set(includeUpper ? "lte" : "lt", to);
    }

    rangeNode.remove("from");
    rangeNode.remove("to");
    rangeNode.remove("include_lower");
    rangeNode.remove("include_upper");

    JsonNode relation = rangeNode.get("relation");
    if (relation != null && relation.isTextual() && "within".equals(relation.asText())) {
      rangeNode.remove("relation");
    }
  }

  private static void normalizeGeoDistance(ObjectNode node) {
    JsonNode geoDistance = node.get("geo_distance");
    if (!(geoDistance instanceof ObjectNode)) {
      return;
    }

    ObjectNode geoDistanceNode = (ObjectNode) geoDistance;

    JsonNode coordinates = geoDistanceNode.get("coordinates");
    if (coordinates != null) {
      if (coordinates.isArray() && coordinates.size() == 2) {
        geoDistanceNode.put("__coord_lat", coordinates.get(1).asDouble());
        geoDistanceNode.put("__coord_lon", coordinates.get(0).asDouble());
        geoDistanceNode.remove("coordinates");
      } else if (coordinates.isObject()) {
        JsonNode lat = coordinates.get("lat");
        JsonNode lon = coordinates.get("lon");
        if (lat != null && lon != null && lat.isNumber() && lon.isNumber()) {
          geoDistanceNode.put("__coord_lat", lat.asDouble());
          geoDistanceNode.put("__coord_lon", lon.asDouble());
          geoDistanceNode.remove("coordinates");
        }
      }
    }

    Double meters = parseDistanceMeters(geoDistanceNode.get("distance"));
    if (meters != null) {
      geoDistanceNode.put("__distance_meters", meters);
      geoDistanceNode.remove("distance");
    }

    JsonNode distanceType = geoDistanceNode.get("distance_type");
    if (distanceType != null && distanceType.isTextual() && "arc".equals(distanceType.asText())) {
      geoDistanceNode.remove("distance_type");
    }

    JsonNode validationMethod = geoDistanceNode.get("validation_method");
    if (validationMethod != null
        && validationMethod.isTextual()
        && "STRICT".equals(validationMethod.asText())) {
      geoDistanceNode.remove("validation_method");
    }

    JsonNode ignoreUnmapped = geoDistanceNode.get("ignore_unmapped");
    if (ignoreUnmapped != null && ignoreUnmapped.isBoolean() && !ignoreUnmapped.asBoolean()) {
      geoDistanceNode.remove("ignore_unmapped");
    }
  }

  private static Double parseDistanceMeters(JsonNode distanceNode) {
    if (distanceNode == null || distanceNode.isNull()) {
      return null;
    }
    if (distanceNode.isNumber()) {
      return distanceNode.asDouble();
    }
    if (!distanceNode.isTextual()) {
      return null;
    }

    String text = distanceNode.asText().trim().toLowerCase();
    try {
      if (text.endsWith("km")) {
        return Double.parseDouble(text.substring(0, text.length() - 2).trim()) * 1000d;
      }
      if (text.endsWith("m")) {
        return Double.parseDouble(text.substring(0, text.length() - 1).trim());
      }
      return Double.parseDouble(text);
    } catch (NumberFormatException ex) {
      return null;
    }
  }

  private static void normalizeGeoShape(ObjectNode node) {
    JsonNode geoShape = node.get("geo_shape");
    if (!(geoShape instanceof ObjectNode)) {
      return;
    }

    ObjectNode geoShapeNode = (ObjectNode) geoShape;
    JsonNode ignoreUnmapped = geoShapeNode.get("ignore_unmapped");
    if (ignoreUnmapped != null && ignoreUnmapped.isBoolean() && !ignoreUnmapped.asBoolean()) {
      geoShapeNode.remove("ignore_unmapped");
    }
  }

  @Test
  public void testEqualsPredicate() throws QueryBuildingException {
    Predicate p = new EqualsPredicate<>(PARAM, "value", false);
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"term\" : {\n"
            + "          \"catalogNumber\" : {\n"
            + "            \"value\" : \"value\"\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testEqualsPredicateMatchVerbatim() throws QueryBuildingException {
    Predicate p = new EqualsPredicate<>(PARAM, "value", true);
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"term\" : {\n"
            + "          \"catalogNumber.verbatim\" : {\n"
            + "            \"value\" : \"value\"\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testEqualsDatePredicate() throws QueryBuildingException {
    Predicate p = new EqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "2021-09-16", false);
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"eventDate\" : {\n"
            + "            \"from\" : \"2021-09-16\",\n"
            + "            \"to\" : \"2021-09-17\",\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : false,\n"
            + "            \"relation\" : \"within\"\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testEqualsRangePredicate() throws QueryBuildingException {
    Predicate p = new EqualsPredicate<>(OccurrenceSearchParameter.ELEVATION, "-20.0,600", false);
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"elevation\" : {\n"
            + "            \"from\" : -20.0,\n"
            + "            \"to\" : 600.0,\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : true\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);

    p = new EqualsPredicate<>(OccurrenceSearchParameter.ELEVATION, "*,600", false);
    query = occurrenceVisitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"elevation\" : {\n"
            + "            \"from\" : null,\n"
            + "            \"to\" : 600.0,\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : true\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);

    p = new EqualsPredicate<>(OccurrenceSearchParameter.ELEVATION, "-20.0,*", false);
    query = occurrenceVisitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"elevation\" : {\n"
            + "            \"from\" : -20.0,\n"
            + "            \"to\" : null,\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : true\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testEqualsDateRangePredicate() throws QueryBuildingException {
    // Occurrences will be returned if the occurrence date/date range is
    // *completely within* the query date or date range.
    Predicate p =
      new EqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "2023-01-11", false);
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
      "{\n"
        + "  \"bool\" : {\n"
        + "    \"filter\" : [\n"
        + "      {\n"
        + "        \"range\" : {\n"
        + "          \"eventDate\" : {\n"
        + "            \"from\" : \"2023-01-11\",\n"
        + "            \"to\" : \"2023-01-12\",\n"
        + "            \"include_lower\" : true,\n"
        + "            \"include_upper\" : false,\n"
        + "            \"relation\" : \"within\"\n"
        + "          }\n"
        + "        }\n"
        + "      }\n"
        + "    ]\n"
        + "  }\n"
        + "}";
    assertJsonEquals(expectedQuery, query);

    p = new EqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "1980-02,2021-09-16", false);
    query = occurrenceVisitor.buildQuery(p);
    expectedQuery =
      "{\n"
        + "  \"bool\" : {\n"
        + "    \"filter\" : [\n"
        + "      {\n"
        + "        \"range\" : {\n"
        + "          \"eventDate\" : {\n"
        + "            \"from\" : \"1980-02-01\",\n"
        + "            \"to\" : \"2021-09-17\",\n"
        + "            \"include_lower\" : true,\n"
        + "            \"include_upper\" : false,\n"
        + "            \"relation\" : \"within\"\n"
        + "          }\n"
        + "        }\n"
        + "      }\n"
        + "    ]\n"
        + "  }\n"
        + "}";
    assertJsonEquals(expectedQuery, query);

    p = new EqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "1980", false);
    query = occurrenceVisitor.buildQuery(p);
    expectedQuery =
      "{\n"
        + "  \"bool\" : {\n"
        + "    \"filter\" : [\n"
        + "      {\n"
        + "        \"range\" : {\n"
        + "          \"eventDate\" : {\n"
        + "            \"from\" : \"1980-01-01\",\n"
        + "            \"to\" : \"1981-01-01\",\n"
        + "            \"include_lower\" : true,\n"
        + "            \"include_upper\" : false,\n"
        + "            \"relation\" : \"within\"\n"
        + "          }\n"
        + "        }\n"
        + "      }\n"
        + "    ]\n"
        + "  }\n"
        + "}";
    assertJsonEquals(expectedQuery, query);

    p = new EqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "2023-01", false);
    query = occurrenceVisitor.buildQuery(p);
    expectedQuery =
      "{\n"
        + "  \"bool\" : {\n"
        + "    \"filter\" : [\n"
        + "      {\n"
        + "        \"range\" : {\n"
        + "          \"eventDate\" : {\n"
        + "            \"from\" : \"2023-01-01\",\n"
        + "            \"to\" : \"2023-02-01\",\n"
        + "            \"include_lower\" : true,\n"
        + "            \"include_upper\" : false,\n"
        + "            \"relation\" : \"within\"\n"
        + "          }\n"
        + "        }\n"
        + "      }\n"
        + "    ]\n"
        + "  }\n"
        + "}";
    assertJsonEquals(expectedQuery, query);

    p = new EqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "2023-01-1", false);
    query = occurrenceVisitor.buildQuery(p);
    expectedQuery =
      "{\n"
        + "  \"bool\" : {\n"
        + "    \"filter\" : [\n"
        + "      {\n"
        + "        \"range\" : {\n"
        + "          \"eventDate\" : {\n"
        + "            \"from\" : \"2023-01-01\",\n"
        + "            \"to\" : \"2023-01-02\",\n"
        + "            \"include_lower\" : true,\n"
        + "            \"include_upper\" : false,\n"
        + "            \"relation\" : \"within\"\n"
        + "          }\n"
        + "        }\n"
        + "      }\n"
        + "    ]\n"
        + "  }\n"
        + "}";
    assertJsonEquals(expectedQuery, query);

    p = new EqualsPredicate<>(OccurrenceSearchParameter.EVENT_DATE, "1980,1990-05-06", false);
    query = occurrenceVisitor.buildQuery(p);
    expectedQuery =
      "{\n"
        + "  \"bool\" : {\n"
        + "    \"filter\" : [\n"
        + "      {\n"
        + "        \"range\" : {\n"
        + "          \"eventDate\" : {\n"
        + "            \"from\" : \"1980-01-01\",\n"
        + "            \"to\" : \"1990-05-07\",\n"
        + "            \"include_lower\" : true,\n"
        + "            \"include_upper\" : false,\n"
        + "            \"relation\" : \"within\"\n"
        + "          }\n"
        + "        }\n"
        + "      }\n"
        + "    ]\n"
        + "  }\n"
        + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testGreaterThanOrEqualPredicate() throws QueryBuildingException {
    Predicate p = new GreaterThanOrEqualsPredicate<>(OccurrenceSearchParameter.ELEVATION, "222");
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"elevation\" : {\n"
            + "            \"from\" : \"222\",\n"
            + "            \"to\" : null,\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : true\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);

    p =
        new GreaterThanOrEqualsPredicate<>(
            OccurrenceSearchParameter.LAST_INTERPRETED, "2021-09-16");
    query = occurrenceVisitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"created\" : {\n"
            + "            \"from\" : \"2021-09-16\",\n"
            + "            \"to\" : null,\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : true\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);

    p = new GreaterThanOrEqualsPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, "2021");
    query = occurrenceVisitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"created\" : {\n"
            + "            \"from\" : \"2021\",\n"
            + "            \"to\" : null,\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : true\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testGreaterThanPredicate() throws QueryBuildingException {
    Predicate p = new GreaterThanPredicate<>(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"elevation\" : {\n"
            + "            \"from\" : \"1000\",\n"
            + "            \"to\" : null,\n"
            + "            \"include_lower\" : false,\n"
            + "            \"include_upper\" : true\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);

    p = new GreaterThanPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, "2021-09-16");
    query = occurrenceVisitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"created\" : {\n"
            + "            \"from\" : \"2021-09-16\",\n"
            + "            \"to\" : null,\n"
            + "            \"include_lower\" : false,\n"
            + "            \"include_upper\" : true\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);

    p = new GreaterThanPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, "2021");
    query = occurrenceVisitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"created\" : {\n"
            + "            \"from\" : \"2021\",\n"
            + "            \"to\" : null,\n"
            + "            \"include_lower\" : false,\n"
            + "            \"include_upper\" : true\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testLessThanOrEqualPredicate() throws QueryBuildingException {
    Predicate p = new LessThanOrEqualsPredicate<>(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"elevation\" : {\n"
            + "            \"from\" : null,\n"
            + "            \"to\" : \"1000\",\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : true\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);

    p = new LessThanOrEqualsPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, "2021-10-25");
    query = occurrenceVisitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"created\" : {\n"
            + "            \"from\" : null,\n"
            + "            \"to\" : \"2021-10-25\",\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : true\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);

    p = new LessThanOrEqualsPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, "2021");
    query = occurrenceVisitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"created\" : {\n"
            + "            \"from\" : null,\n"
            + "            \"to\" : \"2021\",\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : true\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testLessThanPredicate() throws QueryBuildingException {
    Predicate p = new LessThanPredicate<>(OccurrenceSearchParameter.ELEVATION, "1000");
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"elevation\" : {\n"
            + "            \"from\" : null,\n"
            + "            \"to\" : \"1000\",\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : false\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);

    p = new LessThanPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, "2021-10-25");
    query = occurrenceVisitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"created\" : {\n"
            + "            \"from\" : null,\n"
            + "            \"to\" : \"2021-10-25\",\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : false\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);

    p = new LessThanPredicate<>(OccurrenceSearchParameter.LAST_INTERPRETED, "2021");
    query = occurrenceVisitor.buildQuery(p);
    expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"range\" : {\n"
            + "          \"created\" : {\n"
            + "            \"from\" : null,\n"
            + "            \"to\" : \"2021\",\n"
            + "            \"include_lower\" : true,\n"
            + "            \"include_upper\" : false\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testConjunctionPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new EqualsPredicate<>(PARAM2, "value_2", false);
    Predicate p3 = new GreaterThanOrEqualsPredicate<>(OccurrenceSearchParameter.MONTH, "12");
    Predicate p = new ConjunctionPredicate(Arrays.asList(p1, p2, p3));
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"term\" : {\n"
            + "                \"catalogNumber\" : {\n"
            + "                  \"value\" : \"value_1\"\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"term\" : {\n"
            + "                \"institutionCode\" : {\n"
            + "                  \"value\" : \"value_2\"\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"range\" : {\n"
            + "                \"month\" : {\n"
            + "                  \"from\" : \"12\",\n"
            + "                  \"to\" : null,\n"
            + "                  \"include_lower\" : true,\n"
            + "                  \"include_upper\" : true\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testDisjunctionPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new EqualsPredicate<>(PARAM2, "value_2", false);
    Predicate p3 = new EqualsPredicate<>(PARAM, "value_3", false);

    DisjunctionPredicate p = new DisjunctionPredicate(Arrays.asList(p1, p2, p3));
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"term\" : {\n"
            + "                \"institutionCode\" : {\n"
            + "                  \"value\" : \"value_2\"\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"terms\" : {\n"
            + "          \"catalogNumber\" : [\n"
            + "            \"value_3\",\n"
            + "            \"value_1\"\n"
            + "          ]\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testDisjunctionMatchCasePredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new EqualsPredicate<>(PARAM, "value_2", false);

    Predicate p3 = new EqualsPredicate<>(PARAM, "value_3", true);
    Predicate p4 = new EqualsPredicate<>(PARAM, "value_4", true);

    DisjunctionPredicate p = new DisjunctionPredicate(Arrays.asList(p1, p2, p3, p4));
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"should\" : [\n"
            + "      {\n"
            + "        \"terms\" : {\n"
            + "          \"catalogNumber\" : [\n"
            + "            \"value_2\",\n"
            + "            \"value_1\"\n"
            + "          ]\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"terms\" : {\n"
            + "          \"catalogNumber.verbatim\" : [\n"
            + "            \"value_4\",\n"
            + "            \"value_3\"\n"
            + "          ]\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testInPredicate() throws QueryBuildingException {
    Predicate p =
        new InPredicate<>(PARAM, Arrays.asList("value_1", "value_2", "value_3"), false);
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"terms\" : {\n"
            + "          \"catalogNumber\" : [\n"
            + "            \"value_1\",\n"
            + "            \"value_2\",\n"
            + "            \"value_3\"\n"
            + "          ]\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testComplexInPredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 =
        new InPredicate<>(PARAM, Arrays.asList("value_1", "value_2", "value_3"), false);
    Predicate p3 = new EqualsPredicate<>(PARAM2, "value_2", false);
    Predicate p = new ConjunctionPredicate(Arrays.asList(p1, p2, p3));
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"term\" : {\n"
            + "                \"catalogNumber\" : {\n"
            + "                  \"value\" : \"value_1\"\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"terms\" : {\n"
            + "                \"catalogNumber\" : [\n"
            + "                  \"value_1\",\n"
            + "                  \"value_2\",\n"
            + "                  \"value_3\"\n"
            + "                ]\n"
            + "              }\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"term\" : {\n"
            + "                \"institutionCode\" : {\n"
            + "                  \"value\" : \"value_2\"\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testNotPredicate() throws QueryBuildingException {
    Predicate p = new NotPredicate(new EqualsPredicate<>(PARAM, "value", false));
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"must_not\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"term\" : {\n"
            + "                \"catalogNumber\" : {\n"
            + "                  \"value\" : \"value\"\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testNotPredicateComplex() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new EqualsPredicate<>(PARAM2, "value_2", false);

    ConjunctionPredicate cp = new ConjunctionPredicate(Arrays.asList(p1, p2));

    Predicate p = new NotPredicate(cp);
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"must_not\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"term\" : {\n"
            + "                      \"catalogNumber\" : {\n"
            + "                        \"value\" : \"value_1\"\n"
            + "                      }\n"
            + "                    }\n"
            + "                  }\n"
            + "                ]\n"
            + "              }\n"
            + "            },\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"term\" : {\n"
            + "                      \"institutionCode\" : {\n"
            + "                        \"value\" : \"value_2\"\n"
            + "                      }\n"
            + "                    }\n"
            + "                  }\n"
            + "                ]\n"
            + "              }\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testLikePredicate() throws QueryBuildingException {
    // NB: ? and * are wildcards (as in ES).  SQL-like _ and % are literal.
    LikePredicate<OccurrenceSearchParameter> likePredicate =
        new LikePredicate<>(PARAM, "v?l*ue_%", false);
    String query = occurrenceVisitor.buildQuery(likePredicate);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"wildcard\" : {\n"
            + "          \"catalogNumber\" : {\n"
            + "            \"value\" : \"v?l*ue_%\"\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testLikeVerbatimPredicate() throws QueryBuildingException {
    LikePredicate<OccurrenceSearchParameter> likePredicate =
        new LikePredicate<>(PARAM, "v?l*ue_%", true);
    String query = occurrenceVisitor.buildQuery(likePredicate);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"wildcard\" : {\n"
            + "          \"catalogNumber.verbatim\" : {\n"
            + "            \"value\" : \"v?l*ue_%\"\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testComplexLikePredicate() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new LikePredicate<>(PARAM, "value_1*", false);
    Predicate p3 = new EqualsPredicate<>(PARAM2, "value_2", false);
    Predicate p = new ConjunctionPredicate(Arrays.asList(p1, p2, p3));
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"term\" : {\n"
            + "                \"catalogNumber\" : {\n"
            + "                  \"value\" : \"value_1\"\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"wildcard\" : {\n"
            + "                \"catalogNumber\" : {\n"
            + "                  \"value\" : \"value_1*\"\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"term\" : {\n"
            + "                \"institutionCode\" : {\n"
            + "                  \"value\" : \"value_2\"\n"
            + "                }\n"
            + "              }\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testIsNotNullPredicate() throws QueryBuildingException {
    Predicate p = new IsNotNullPredicate<>(PARAM);
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"exists\" : {\n"
            + "          \"field\" : \"catalogNumber\"\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testIsNullPredicate() throws QueryBuildingException {
    Predicate p = new IsNullPredicate<>(PARAM);
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"must_not\" : [\n"
            + "            {\n"
            + "              \"exists\" : {\n"
            + "                \"field\" : \"catalogNumber\"\n"
            + "              }\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testWithinPredicate() throws QueryBuildingException {
    final String wkt = "POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))";
    Predicate p = new WithinPredicate(wkt);
    String query = occurrenceVisitor.buildQuery(p);
    assertNotNull(query);
  }

  @Test
  public void testGeoDistancePredicate() throws QueryBuildingException {
    Predicate p = new GeoDistancePredicate("10", "20", "10km");
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"geo_distance\" : {\n"
            + "          \"coordinates\" : [\n"
            + "            20.0,\n"
            + "            10.0\n"
            + "          ],\n"
            + "          \"distance\" : 10000.0,\n"
            + "          \"distance_type\" : \"arc\",\n"
            + "          \"validation_method\" : \"STRICT\",\n"
            + "          \"ignore_unmapped\" : false\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testComplexPredicateOne() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new LikePredicate<>(PARAM, "value_1*", false);
    Predicate p3 = new EqualsPredicate<>(PARAM2, "value_2", false);
    Predicate pcon = new ConjunctionPredicate(Arrays.asList(p1, p2, p3));
    Predicate pdis = new DisjunctionPredicate(Arrays.asList(p1, pcon));
    Predicate p = new NotPredicate(pdis);
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"must_not\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"should\" : [\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"term\" : {\n"
            + "                      \"catalogNumber\" : {\n"
            + "                        \"value\" : \"value_1\"\n"
            + "                      }\n"
            + "                    }\n"
            + "                  }\n"
            + "                ]\n"
            + "              }\n"
            + "            },\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"filter\" : [\n"
            + "                        {\n"
            + "                          \"term\" : {\n"
            + "                            \"catalogNumber\" : {\n"
            + "                              \"value\" : \"value_1\"\n"
            + "                            }\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ]\n"
            + "                    }\n"
            + "                  },\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"filter\" : [\n"
            + "                        {\n"
            + "                          \"wildcard\" : {\n"
            + "                            \"catalogNumber\" : {\n"
            + "                              \"value\" : \"value_1*\"\n"
            + "                            }\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ]\n"
            + "                    }\n"
            + "                  },\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"filter\" : [\n"
            + "                        {\n"
            + "                          \"term\" : {\n"
            + "                            \"institutionCode\" : {\n"
            + "                              \"value\" : \"value_2\"\n"
            + "                            }\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ]\n"
            + "                    }\n"
            + "                  }\n"
            + "                ]\n"
            + "              }\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testComplexPredicateTwo() throws QueryBuildingException {
    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new LikePredicate<>(PARAM, "value_1*", false);
    Predicate p3 = new EqualsPredicate<>(PARAM2, "value_2", false);

    Predicate p4 = new DisjunctionPredicate(Arrays.asList(p1, p3));
    Predicate p5 = new ConjunctionPredicate(Arrays.asList(p1, p2));

    Predicate p = new ConjunctionPredicate(Arrays.asList(p4, new NotPredicate(p5)));
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"should\" : [\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"term\" : {\n"
            + "                      \"catalogNumber\" : {\n"
            + "                        \"value\" : \"value_1\"\n"
            + "                      }\n"
            + "                    }\n"
            + "                  }\n"
            + "                ]\n"
            + "              }\n"
            + "            },\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"term\" : {\n"
            + "                      \"institutionCode\" : {\n"
            + "                        \"value\" : \"value_2\"\n"
            + "                      }\n"
            + "                    }\n"
            + "                  }\n"
            + "                ]\n"
            + "              }\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"must_not\" : [\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"filter\" : [\n"
            + "                        {\n"
            + "                          \"term\" : {\n"
            + "                            \"catalogNumber\" : {\n"
            + "                              \"value\" : \"value_1\"\n"
            + "                            }\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ]\n"
            + "                    }\n"
            + "                  },\n"
            + "                  {\n"
            + "                    \"bool\" : {\n"
            + "                      \"filter\" : [\n"
            + "                        {\n"
            + "                          \"wildcard\" : {\n"
            + "                            \"catalogNumber\" : {\n"
            + "                              \"value\" : \"value_1*\"\n"
            + "                            }\n"
            + "                          }\n"
            + "                        }\n"
            + "                      ]\n"
            + "                    }\n"
            + "                  }\n"
            + "                ]\n"
            + "              }\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testComplexPredicateThree() throws QueryBuildingException {
    final String wkt = "POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))";

    Predicate p1 = new EqualsPredicate<>(PARAM, "value_1", false);
    Predicate p2 = new LikePredicate<>(PARAM, "value_1*", false);
    Predicate p3 = new EqualsPredicate<>(PARAM2, "value_2", false);
    Predicate p4 = new WithinPredicate(wkt);

    Predicate p5 = new DisjunctionPredicate(Arrays.asList(p1, p3, p4));
    Predicate p6 = new ConjunctionPredicate(Arrays.asList(p1, p2));

    Predicate p = new ConjunctionPredicate(Arrays.asList(p5, p6));
    String query = occurrenceVisitor.buildQuery(p);
    String expectedQuery =
        "{\n"
            + "  \"bool\" : {\n"
            + "    \"filter\" : [\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"should\" : [\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"term\" : {\n"
            + "                      \"catalogNumber\" : {\n"
            + "                        \"value\" : \"value_1\"\n"
            + "                      }\n"
            + "                    }\n"
            + "                  }\n"
            + "                ]\n"
            + "              }\n"
            + "            },\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"term\" : {\n"
            + "                      \"institutionCode\" : {\n"
            + "                        \"value\" : \"value_2\"\n"
            + "                      }\n"
            + "                    }\n"
            + "                  }\n"
            + "                ]\n"
            + "              }\n"
            + "            },\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"geo_shape\" : {\n"
            + "                      \"scoordinates\" : {\n"
            + "                        \"shape\" : {\n"
            + "                          \"type\" : \"Polygon\",\n"
            + "                          \"coordinates\" : [\n"
            + "                            [\n"
            + "                              [\n"
            + "                                30.0,\n"
            + "                                10.0\n"
            + "                              ],\n"
            + "                              [\n"
            + "                                10.0,\n"
            + "                                20.0\n"
            + "                              ],\n"
            + "                              [\n"
            + "                                20.0,\n"
            + "                                40.0\n"
            + "                              ],\n"
            + "                              [\n"
            + "                                40.0,\n"
            + "                                40.0\n"
            + "                              ],\n"
            + "                              [\n"
            + "                                30.0,\n"
            + "                                10.0\n"
            + "                              ]\n"
            + "                            ]\n"
            + "                          ]\n"
            + "                        },\n"
            + "                        \"relation\" : \"within\"\n"
            + "                      },\n"
            + "                      \"ignore_unmapped\" : false\n"
            + "                    }\n"
            + "                  }\n"
            + "                ]\n"
            + "              }\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      },\n"
            + "      {\n"
            + "        \"bool\" : {\n"
            + "          \"filter\" : [\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"term\" : {\n"
            + "                      \"catalogNumber\" : {\n"
            + "                        \"value\" : \"value_1\"\n"
            + "                      }\n"
            + "                    }\n"
            + "                  }\n"
            + "                ]\n"
            + "              }\n"
            + "            },\n"
            + "            {\n"
            + "              \"bool\" : {\n"
            + "                \"filter\" : [\n"
            + "                  {\n"
            + "                    \"wildcard\" : {\n"
            + "                      \"catalogNumber\" : {\n"
            + "                        \"value\" : \"value_1*\"\n"
            + "                      }\n"
            + "                    }\n"
            + "                  }\n"
            + "                ]\n"
            + "              }\n"
            + "            }\n"
            + "          ]\n"
            + "        }\n"
            + "      }\n"
            + "    ]\n"
            + "  }\n"
            + "}";
    assertJsonEquals(expectedQuery, query);
  }

  @Test
  public void testVocabularyEqualsPredicate() {
    OccurrenceEsFieldMapper esFieldMapper = OccurrenceEsField.buildFieldMapper();
    Arrays.stream(OccurrenceSearchParameter.values())
        .filter(esFieldMapper::isVocabulary)
        .forEach(
            param -> {
              try {
                Predicate p = new EqualsPredicate<>(param, "value", false);
                String searchFieldName = occurrenceEsFieldMapper.getExactMatchFieldName(param);
                String query = occurrenceVisitor.buildQuery(p);
                String expectedQuery =
                    "{\n"
                        + "  \"bool\" : {\n"
                        + "    \"filter\" : [\n"
                        + "      {\n"
                        + "        \"term\" : {\n"
                        + "          \""
                        + searchFieldName
                        + "\" : {\n"
                        + "            \"value\" : \"value\"\n"
                        + "          }\n"
                        + "        }\n"
                        + "      }\n"
                        + "    ]\n"
                        + "  }\n"
                        + "}";
                assertJsonEquals(expectedQuery, query);
              } catch (QueryBuildingException ex) {
                throw new RuntimeException(ex);
              }
            });
  }

  @Test
  public void testAllParametersMapped() {
    for (OccurrenceSearchParameter param : OccurrenceSearchParameter.values()) {
      try {
        Predicate p;
        Object value;
        if (param == OccurrenceSearchParameter.GEOMETRY) {
          value = "POLYGON ((30 10, 10 20, 20 40, 40 40, 30 10))";
          p = new WithinPredicate(value.toString());
        } else if (param == OccurrenceSearchParameter.GEO_DISTANCE) {
          value = "10,10,10km";
          p = new GeoDistancePredicate(DistanceUnit.GeoDistance.parseGeoDistance(value.toString()));
        } else {
          if (param.type().isAssignableFrom(UUID.class)) {
            value = UUID.randomUUID();
          } else if (param.type().isAssignableFrom(Boolean.class)) {
            value = true;
          } else if (param.type().isEnum()) {
            value = param.type().getEnumConstants()[0];
          } else if (param.type().isAssignableFrom(Date.class)) {
            value = "2023-03-02";
          } else if (param.type().isAssignableFrom(IsoDateInterval.class)) {
            value = "2023-03";
          } else {
            // String, Integer and Double
            value = "1";
          }
          p = new EqualsPredicate<>(param, value.toString(), false);
        }
        occurrenceVisitor.buildQuery(p);
      } catch (Exception e) {
        System.err.println(e.getMessage());
        e.printStackTrace();
        fail("Failed for " + param);
      }
    }
  }
}
