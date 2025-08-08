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
package org.gbif.occurrence.search.es;

import static org.gbif.event.search.es.EventEsField.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY;
import static org.gbif.occurrence.search.es.EsQueryUtils.*;
import static org.gbif.occurrence.search.es.OccurrenceEsField.*;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.QueryBuilder;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.event.search.es.EventEsField;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventEsSearchRequestBuilderTest {

  private static final Logger LOG = LoggerFactory.getLogger(EventEsSearchRequestBuilderTest.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String INDEX = "index";
  public static final String DEFAULT_CHECKLIST_KEY = "defaultChecklistKey";

  private final EsSearchRequestBuilder esSearchRequestBuilder =
      new EsSearchRequestBuilder(
          EventEsField.buildFieldMapper(DEFAULT_CHECKLIST_KEY), new ConceptClientMock(), null);

  @Test
  public void humboldtTaxonomyTest() throws Exception {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addParameter(
        OccurrenceSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY, "uk");
    searchRequest.addParameter(
        OccurrenceSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_TAXON_KEY, "tk");
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
                "event.humboldt.targetTaxonomicScope." + DEFAULT_CHECKLIST_KEY + ".usageKey")
            .get(0)
            .get(0)
            .asText());
    assertEquals(
        "tk",
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .findValues(
                "event.humboldt.targetTaxonomicScope." + DEFAULT_CHECKLIST_KEY + ".taxonKeys")
            .get(0)
            .get(0)
            .asText());
  }

  @Test
  public void humboldtTaxonomicIssueTest() throws Exception {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addParameter(OccurrenceSearchParameter.TAXONOMIC_ISSUE, "iss");
    QueryBuilder query =
        esSearchRequestBuilder
            .buildQueryNode(searchRequest)
            .orElseThrow(IllegalArgumentException::new);
    JsonNode jsonQuery = MAPPER.readTree(query.toString());
    assertEquals(
        "iss",
        jsonQuery
            .path(BOOL)
            .path(FILTER)
            .findValues("event.humboldt.targetTaxonomicScope." + DEFAULT_CHECKLIST_KEY + ".issues")
            .get(0)
            .get(0)
            .asText());
  }

  @Test
  public void humboldtDifferentChecklistKeyTest() throws Exception {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addParameter(
        OccurrenceSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY, "uk");
    searchRequest.addParameter(OccurrenceSearchParameter.CHECKLIST_KEY, "key2");
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
            .findValues("event.humboldt.targetTaxonomicScope.key2.usageKey")
            .get(0)
            .get(0)
            .asText());
  }

  @Test
  public void humboldtTaxonomyFacetTest() throws Exception {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addFacets(OccurrenceSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY);
    searchRequest.addFacetPage(
        OccurrenceSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY, 0, 5);
    QueryBuilder query =
        esSearchRequestBuilder
            .buildQueryNode(searchRequest)
            .orElseThrow(IllegalArgumentException::new);
    SearchRequest request = esSearchRequestBuilder.buildSearchRequest(searchRequest, INDEX);
    JsonNode jsonQuery = MAPPER.readTree(request.source().toString());
    JsonNode aggs =
        jsonQuery.path(AGGREGATIONS).path(HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY.name());
    assertEquals(
        String.format(
            HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY.getSearchFieldName(), DEFAULT_CHECKLIST_KEY),
        aggs.path(TERMS).path(FIELD).asText());
    assertEquals(5, aggs.path(TERMS).path(SIZE).asInt());
  }

  @Test
  public void humboldtEventDurationTest() throws Exception {
    OccurrenceSearchRequest searchRequest = new OccurrenceSearchRequest();
    searchRequest.addParameter(OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE, "2");
    searchRequest.addParameter(OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION_UNIT, "minutes");
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
}
