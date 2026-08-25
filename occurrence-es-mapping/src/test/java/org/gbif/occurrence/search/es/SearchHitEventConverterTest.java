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

import org.gbif.api.model.event.Event;
import org.gbif.search.es.event.EventEsFieldMapper;
import org.gbif.search.es.event.SearchHitEventConverter;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import co.elastic.clients.elasticsearch.core.search.Hit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SearchHitEventConverterTest {

  private String readExampleJson() throws Exception {
    String resourcePath = "es-response-example-empty-lists.json";
    InputStream in = getClass().getClassLoader().getResourceAsStream(resourcePath);
    assertNotNull(in, "Example JSON resource not found at " + resourcePath);
    return new String(in.readAllBytes(), StandardCharsets.UTF_8);
  }

  @Test
  void testSearchHitEmptyLists() throws Exception {
    ObjectMapper om = new ObjectMapper();
    String json = readExampleJson();
    JsonNode root = om.readTree(json);

    // Elasticsearch SearchHit.getSourceAsMap() returns the _source object.
    JsonNode firstResult = root.get(0);
    JsonNode sourceNode = firstResult.get("_source");
    Map<String, Object> sourceMap =
      om.convertValue(sourceNode, new TypeReference<>() {});

    Hit<Map<String, Object>> hit =
        Hit.of(
            h ->
                h.index("test")
                    .id(firstResult.path("_id").asText("1"))
                    .source(sourceMap));

    // mock the EventEsFieldMapper (converter requires it but tests use the source map directly)
    EventEsFieldMapper mapper = Mockito.mock(EventEsFieldMapper.class);

    // create converter and run
    SearchHitEventConverter converter = new SearchHitEventConverter(mapper, false);
    Event event = converter.apply(hit);

    // basic assertions: conversion produced an Event and id copied from hit
    assertNotNull(event, "Converter should return a non-null Event");
    assertEquals(hit.id(), event.getId(), "Event id should be set from SearchHit.getId()");
    assertNotNull(event.getVerbatimFields(), "Verbatim fields map should be non-null");

    assertEquals(event.getHumboldt().get(0).getTargetLifeStageScope().get(0), "Tadpole",
      "Target life stage scope should be correctly mapped from source");

    assertNull(event.getHumboldt().get(0).getExcludedLifeStageScope());
  }
}
