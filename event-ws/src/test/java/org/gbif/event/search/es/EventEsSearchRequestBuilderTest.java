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
package org.gbif.event.search.es;

import static org.gbif.occurrence.search.es.EsQueryUtils.*;
import static org.gbif.search.es.event.EventEsField.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.List;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.QueryBuilder;
import org.gbif.api.model.event.search.EventPredicateSearchRequest;
import org.gbif.api.model.event.search.EventSearchParameter;
import org.gbif.api.model.event.search.EventSearchRequest;
import org.gbif.api.model.predicate.ConjunctionPredicate;
import org.gbif.api.model.predicate.DisjunctionPredicate;
import org.gbif.api.model.predicate.EqualsPredicate;
import org.gbif.api.model.predicate.InPredicate;
import org.gbif.api.model.predicate.NotPredicate;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.search.es.event.EventEsField;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventEsSearchRequestBuilderTest {

  private static final Logger LOG = LoggerFactory.getLogger(EventEsSearchRequestBuilderTest.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String INDEX = "index";
  public static final String DEFAULT_CHECKLIST_KEY = "defaultChecklistKey";

  private final EventEsSearchRequestBuilder esSearchRequestBuilder =
      new EventEsSearchRequestBuilder(
          EventEsField.buildFieldMapper(), new ConceptClientMock(), null, DEFAULT_CHECKLIST_KEY);

  @Test
  public void humboldtTaxonomyTest() throws Exception {
    EventSearchRequest searchRequest = new EventSearchRequest();
    searchRequest.addParameter(
        EventSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY, "uk");
    searchRequest.addParameter(
        EventSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_TAXON_KEY, "tk");
    QueryBuilder query =
        esSearchRequestBuilder
            .buildQueryNode(searchRequest)
            .orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    assertEquals(
        "uk",
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .findValues(
                "event.humboldt.targetTaxonomicScope." + DEFAULT_CHECKLIST_KEY + ".usage.key")
            .get(0)
            .get(VALUE)
            .asText());
    assertEquals(
        "tk",
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .findValues(
                "event.humboldt.targetTaxonomicScope." + DEFAULT_CHECKLIST_KEY + ".taxonKeys")
            .get(0)
            .get(VALUE)
            .asText());
  }

  @Test
  public void humboldtTaxonomicIssueTest() throws Exception {
    EventSearchRequest searchRequest = new EventSearchRequest();
    searchRequest.addParameter(EventSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_ISSUE, "TAXON_MATCH_NONE");
    QueryBuilder query =
        esSearchRequestBuilder
            .buildQueryNode(searchRequest)
            .orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    assertFalse(jsonQuery.path(BOOL).path(FILTER).findPath(NESTED).isEmpty());
    assertEquals(
        "event.humboldt", jsonQuery.path(BOOL).path(FILTER).findPath(NESTED).path(PATH).asText());
    assertEquals(
        "TAXON_MATCH_NONE",
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .findValues("event.humboldt.targetTaxonomicScope." + DEFAULT_CHECKLIST_KEY + ".issues")
            .get(0)
            .get(VALUE)
            .asText());
  }

  @Test
  public void humboldtDifferentChecklistKeyTest() throws Exception {
    EventSearchRequest searchRequest = new EventSearchRequest();
    searchRequest.addParameter(
        EventSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY, "uk");
    searchRequest.addParameter(EventSearchParameter.CHECKLIST_KEY, "key2");
    QueryBuilder query =
        esSearchRequestBuilder
            .buildQueryNode(searchRequest)
            .orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    assertEquals(
        "uk",
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .findValues("event.humboldt.targetTaxonomicScope.key2.usage.key")
            .get(0)
            .get(VALUE)
            .asText());
  }

  @Test
  public void humboldtTaxonomyFacetTest() throws Exception {
    EventSearchRequest searchRequest = new EventSearchRequest();
    searchRequest.addFacets(EventSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY);
    searchRequest.addFacetPage(
        EventSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY, 0, 5);
    QueryBuilder query =
        esSearchRequestBuilder
            .buildQueryNode(searchRequest)
            .orElseThrow(IllegalArgumentException::new);
    SearchRequest request = esSearchRequestBuilder.buildSearchRequest(searchRequest, INDEX);
    JsonNode jsonQuery = MAPPER.readTree(request.source().toString());
    JsonNode aggs =
        jsonQuery.path(AGGREGATIONS).path(HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY.name());
    assertEquals("event.humboldt", aggs.path(NESTED).path(PATH).asText());
    assertEquals(
        String.format(
            HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY.getSearchFieldName(), DEFAULT_CHECKLIST_KEY),
        aggs.path(AGGREGATIONS)
            .path(HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY.name())
            .path(TERMS)
            .path(FIELD)
            .asText());
    assertEquals(
        5,
        aggs.path(AGGREGATIONS)
            .path(HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY.name())
            .path(TERMS)
            .path(SIZE)
            .asInt());
  }

  @Test
  public void humboldtEventDurationTest() throws Exception {
    EventSearchRequest searchRequest = new EventSearchRequest();
    searchRequest.addParameter(EventSearchParameter.HUMBOLDT_EVENT_DURATION, "2");
    QueryBuilder query =
        esSearchRequestBuilder
            .buildQueryNode(searchRequest)
            .orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    assertEquals(
        2,
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .findValues("event.humboldt.eventDurationValueInMinutes")
            .get(0)
            .get("value")
            .asInt());
  }

  @Test
  public void conjunctionNestedPredicateTest() throws Exception {
    Predicate p1 = new EqualsPredicate<>(EventSearchParameter.HUMBOLDT_SITE_COUNT, "1", false);
    Predicate p2 =
        new EqualsPredicate<>(
            EventSearchParameter.HUMBOLDT_TOTAL_AREA_SAMPLED_UNIT,
            "kilometers distance not traveled",
            false);
    Predicate p3 = new ConjunctionPredicate(Arrays.asList(p1, p2));
    EventPredicateSearchRequest searchRequest = new EventPredicateSearchRequest();
    searchRequest.setPredicate(p3);
    QueryBuilder query =
        esSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    assertEquals(2, jsonQuery.findPath(NESTED).findPath(FILTER).size());
  }

  @Test
  public void conjunctionNestedMixedPredicateTest() throws Exception {
    Predicate p1 = new EqualsPredicate<>(EventSearchParameter.EVENT_TYPE, "Event", false);
    Predicate p2 =
        new EqualsPredicate<>(
            EventSearchParameter.HUMBOLDT_TOTAL_AREA_SAMPLED_UNIT,
            "kilometers distance not traveled",
            false);
    Predicate p3 = new ConjunctionPredicate(Arrays.asList(p1, p2));
    EventPredicateSearchRequest searchRequest = new EventPredicateSearchRequest();
    searchRequest.setPredicate(p3);
    QueryBuilder query =
        esSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    assertEquals(2, jsonQuery.findPath(FILTER).size());
    assertNotNull(jsonQuery.findPath(FILTER).findPath(NESTED));
  }

  @Test
  public void disjunctionNestedPredicateTest() throws Exception {
    Predicate p1 = new EqualsPredicate<>(EventSearchParameter.HUMBOLDT_SITE_COUNT, "1", false);
    Predicate p2 =
        new EqualsPredicate<>(
            EventSearchParameter.HUMBOLDT_TOTAL_AREA_SAMPLED_UNIT,
            "kilometers distance not traveled",
            false);
    Predicate p3 = new DisjunctionPredicate(Arrays.asList(p1, p2));
    EventPredicateSearchRequest searchRequest = new EventPredicateSearchRequest();
    searchRequest.setPredicate(p3);
    QueryBuilder query =
        esSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    assertEquals(2, jsonQuery.findPath(NESTED).findPath(SHOULD).size());
  }

  @Test
  public void disjunctionNestedMixedPredicateTest() throws Exception {
    Predicate p1 = new EqualsPredicate<>(EventSearchParameter.EVENT_TYPE, "Event", false);
    Predicate p2 =
        new EqualsPredicate<>(
            EventSearchParameter.HUMBOLDT_TOTAL_AREA_SAMPLED_UNIT,
            "kilometers distance not traveled",
            false);
    Predicate p3 = new DisjunctionPredicate(Arrays.asList(p1, p2));
    EventPredicateSearchRequest searchRequest = new EventPredicateSearchRequest();
    searchRequest.setPredicate(p3);
    QueryBuilder query =
        esSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    assertEquals(2, jsonQuery.findPath(SHOULD).size());
    assertNotNull(jsonQuery.findPath(SHOULD).findPath(NESTED));
  }

  @Test
  public void notNestedPredicateTest() throws Exception {
    Predicate p1 = new EqualsPredicate<>(EventSearchParameter.HUMBOLDT_SITE_COUNT, "1", false);
    Predicate p2 =
        new EqualsPredicate<>(
            EventSearchParameter.HUMBOLDT_TOTAL_AREA_SAMPLED_UNIT,
            "kilometers distance not traveled",
            false);
    Predicate p3 = new DisjunctionPredicate(Arrays.asList(p1, p2));
    EventPredicateSearchRequest searchRequest = new EventPredicateSearchRequest();
    searchRequest.setPredicate(new NotPredicate(p3));
    QueryBuilder query =
        esSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    assertEquals(
        2, jsonQuery.findPath(MUST_NOT).findPath(SHOULD).findPath(NESTED).findPath(SHOULD).size());
    assertNotNull(jsonQuery.findPath(MUST_NOT));
  }

  @Test
  public void notNestedMixedPredicateTest() throws Exception {
    Predicate p1 = new EqualsPredicate<>(EventSearchParameter.EVENT_TYPE, "Event", false);
    Predicate p11 =
        new EqualsPredicate<>(
            EventSearchParameter.HUMBOLDT_VERBATIM_SITE_NAMES, "L10510092", false);
    Predicate p2 =
        new EqualsPredicate<>(
            EventSearchParameter.HUMBOLDT_TOTAL_AREA_SAMPLED_UNIT,
            "kilometers distance not traveled",
            false);
    Predicate p3 = new DisjunctionPredicate(Arrays.asList(p1, p11, p2));
    EventPredicateSearchRequest searchRequest = new EventPredicateSearchRequest();
    searchRequest.setPredicate(new NotPredicate(p3));
    QueryBuilder query =
        esSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    assertEquals(2, jsonQuery.findPath(MUST_NOT).findPath(SHOULD).size());
    assertEquals(
        2, jsonQuery.findPath(MUST_NOT).findPath(SHOULD).findPath(NESTED).findPath(SHOULD).size());
    assertNotNull(jsonQuery.findPath(MUST_NOT));
  }

  @Test
  public void inNestedPredicateTest() throws Exception {
    Predicate p1 =
        new InPredicate<>(EventSearchParameter.HUMBOLDT_SITE_COUNT, List.of("1", "3"), false);
    Predicate p2 =
        new InPredicate<>(
            EventSearchParameter.HUMBOLDT_TOTAL_AREA_SAMPLED_UNIT,
            List.of("kilometers distance not traveled"),
            false);
    Predicate p3 = new ConjunctionPredicate(Arrays.asList(p1, p2));
    EventPredicateSearchRequest searchRequest = new EventPredicateSearchRequest();
    searchRequest.setPredicate(p3);
    QueryBuilder query =
        esSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    assertEquals(2, jsonQuery.findPath(NESTED).findPath(FILTER).size());
  }

  @Test
  public void humboldtEventDurationPredicateTest() throws Exception {
    Predicate p1 = new EqualsPredicate<>(EventSearchParameter.HUMBOLDT_EVENT_DURATION, "2", false);
    EventPredicateSearchRequest searchRequest = new EventPredicateSearchRequest();
    searchRequest.setPredicate(p1);

    QueryBuilder query =
        esSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    assertEquals(
        2,
        jsonQuery
            .findPath(NESTED)
            .findValues("event.humboldt.eventDurationValueInMinutes")
            .get(0)
            .get("value")
            .asInt());
  }

  @Test
  public void humboldtTaxonomyPredicateTest() throws Exception {
    Predicate p1 =
        new EqualsPredicate<>(
            EventSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY,
            "uk",
            false,
            DEFAULT_CHECKLIST_KEY);
    Predicate p2 =
        new EqualsPredicate<>(
            EventSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_TAXON_KEY,
            "tk",
            false,
            DEFAULT_CHECKLIST_KEY);
    EventPredicateSearchRequest searchRequest = new EventPredicateSearchRequest();
    searchRequest.setPredicate(new ConjunctionPredicate(List.of(p1, p2)));

    QueryBuilder query =
        esSearchRequestBuilder.buildQuery(searchRequest).orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    assertEquals(
        "uk",
        jsonQuery
            .findParent(NESTED)
            .findValues(
                "event.humboldt.targetTaxonomicScope." + DEFAULT_CHECKLIST_KEY + ".usage.key")
            .get(0)
            .get(VALUE)
            .asText());
    assertEquals(
        "tk",
        jsonQuery
            .findPath(NESTED)
            .findValues(
                "event.humboldt.targetTaxonomicScope." + DEFAULT_CHECKLIST_KEY + ".taxonKeys")
            .get(0)
            .get(VALUE)
            .asText());
  }
}
