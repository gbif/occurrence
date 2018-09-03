package org.gbif.occurrence.search.es;

import com.google.common.collect.Multimap;
import org.apache.http.HttpEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;

import static org.gbif.api.util.SearchTypeValidator.isRange;
import static org.gbif.occurrence.search.es.EsQueryUtils.*;

@Deprecated
public abstract class EsRequestBuilderBase {

  private static final Logger LOG = LoggerFactory.getLogger(EsRequestBuilderBase.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectWriter WRITER = MAPPER.writer();

  protected static final BiFunction<String, Object, ObjectNode> CREATE_NODE =
    (key, value) -> {
      ObjectNode node = createObjectNode();

      if (value instanceof JsonNode) {
        node.put(key, (JsonNode) value);
      } else if (value instanceof Integer) {
        node.put(key, (Integer) value);
      } else {
        node.put(key, (String) value);
      }

      return node;
    };

  protected static ObjectNode buildRangeQuery(OccurrenceEsField esField, String value) {
    String[] values = value.split(RANGE_SEPARATOR);
    if (values.length < 2) {
      return MAPPER.createObjectNode();
    }

    ObjectNode range = MAPPER.createObjectNode();
    range.put(GTE, Double.valueOf(values[0]));
    range.put(LTE, Double.valueOf(values[1]));

    return CREATE_NODE.apply(RANGE, CREATE_NODE.apply(esField.getFieldName(), range));
  }

  protected static Optional<List<ObjectNode>> buildTermQueries(
    Multimap<OccurrenceSearchParameter, String> params) {
    Objects.requireNonNull(params);
    // must term fields
    List<ObjectNode> termQueries = new ArrayList<>();
    for (OccurrenceSearchParameter param : params.keySet()) {
      OccurrenceEsField esField = SEARCH_TO_ES_MAPPING.get(param);
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

    return termQueries.isEmpty() ? Optional.empty() : Optional.of(termQueries);
  }

  protected static Optional<ObjectNode> createTermQuery(
    OccurrenceEsField esField, List<String> parsedValues) {
    if (parsedValues.isEmpty()) {
      return Optional.empty();
    }

    if (parsedValues.size() > 1) {
      // multi term query
      ArrayNode termsArray = MAPPER.createArrayNode();
      parsedValues.forEach(termsArray::add);
      return Optional.of(
        CREATE_NODE.apply(TERMS, CREATE_NODE.apply(esField.getFieldName(), termsArray)));
    }
    // single term
    return Optional.of(
      CREATE_NODE.apply(TERM, CREATE_NODE.apply(esField.getFieldName(), parsedValues.get(0))));
  }

  protected static HttpEntity createEntity(ObjectNode json) {
    try {
      return new NStringEntity(WRITER.writeValueAsString(json));
    } catch (IOException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
  }

  protected static ObjectNode createObjectNode() {
    return MAPPER.createObjectNode();
  }

  protected static ArrayNode createArrayNode() {
    return MAPPER.createArrayNode();
  }
}
