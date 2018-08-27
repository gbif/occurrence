package org.gbif.occurrence.search.es;

import com.google.common.collect.Multimap;
import org.apache.http.HttpEntity;
import org.apache.http.nio.entity.NStringEntity;
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

public abstract class EsRequestBuilderBase {

  private static final Logger LOG = LoggerFactory.getLogger(EsRequestBuilderBase.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final ObjectWriter WRITER = MAPPER.writer();

  protected static final BiFunction<String, Object, ObjectNode> CREATE_NODE =
      (key, value) -> {
        ObjectNode node = createObjectNode();

        if (value instanceof ObjectNode) {
          node.put(key, (ObjectNode) value);
        } else if (value instanceof Integer) {
          node.put(key, (Integer) value);
        } else {
          node.put(key, (String) value);
        }

        return node;
      };

  protected static ObjectNode buildRangeQuery(OccurrenceEsField esField, String value) {
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

  protected static Optional<List<ObjectNode>> buildTermQueries(
      Multimap<OccurrenceSearchParameter, String> params) {
    Objects.requireNonNull(params);
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

    return termQueries.isEmpty() ? Optional.empty() : Optional.of(termQueries);
  }

  protected static Optional<ObjectNode> createTermQuery(
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

  protected static ObjectNode createSingleTermQuery(OccurrenceEsField esField, String parsedValue) {
    ObjectNode termQuery = MAPPER.createObjectNode();
    termQuery.put(esField.getFieldName(), parsedValue);
    ObjectNode term = MAPPER.createObjectNode();
    term.put(TERM, termQuery);
    return term;
  }

  protected static ObjectNode createMultitermQuery(
      OccurrenceEsField esField, List<String> parsedValues) {
    ObjectNode multitermQuery = MAPPER.createObjectNode();
    ArrayNode termsArray = MAPPER.createArrayNode();
    parsedValues.forEach(termsArray::add);
    multitermQuery.put(esField.getFieldName(), termsArray);
    ObjectNode terms = MAPPER.createObjectNode();
    terms.put(TERMS, multitermQuery);
    return terms;
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
