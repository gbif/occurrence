package org.gbif.occurrence.search.es;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.search.SearchHit;
import org.gbif.api.model.event.Event;
import org.gbif.search.es.event.EventEsFieldMapper;
import org.gbif.search.es.event.SearchHitEventConverter;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

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

    // mock a SearchHit that returns our example source
    SearchHit hit = Mockito.mock(SearchHit.class);
    Mockito.when(hit.getSourceAsMap()).thenReturn(sourceMap);
    Mockito.when(hit.getSourceAsString()).thenReturn(sourceNode.toString());
    // propagate id if present, else default to "1"
    Mockito.when(hit.getId()).thenReturn(root.path("_id").asText("1"));

    // mock the EventEsFieldMapper (converter requires it but tests use the source map directly)
    EventEsFieldMapper mapper = Mockito.mock(EventEsFieldMapper.class);

    // create converter and run
    SearchHitEventConverter converter = new SearchHitEventConverter(mapper, false);
    Event event = converter.apply(hit);

    // basic assertions: conversion produced an Event and id copied from hit
    assertNotNull(event, "Converter should return a non-null Event");
    assertEquals(hit.getId(), event.getId(), "Event id should be set from SearchHit.getId()");
    assertNotNull(event.getVerbatimFields(), "Verbatim fields map should be non-null");

    assertEquals(event.getHumboldt().get(0).getTargetLifeStageScope().get(0), "Tadpole",
      "Target life stage scope should be correctly mapped from source");

    assertNull(event.getHumboldt().get(0).getExcludedLifeStageScope());
  }
}
