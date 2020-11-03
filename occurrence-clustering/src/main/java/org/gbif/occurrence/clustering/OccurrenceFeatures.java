package org.gbif.occurrence.clustering;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

/**
 * A wrapper around a row giving typed access to named terms with null handling.
 */
public class OccurrenceFeatures {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  static {
    // required for e.g. correct empty array serializations
    OBJECT_MAPPER.registerModule(new DefaultScalaModule());
  }

  private final Row row;
  private final String prefix; // if field names carry prefixes e.g. t1_scientificName has t1_

  // this hack is only used to correctly serialize fields containing JSON correctly
  private final Set<String> jsonFields;

  /**
   * @param row The underlying DataSet row to expose
   * @param prefix The prefix to strip from field names
   * @param jsonFields Fields in Row already stored as JSON strings supplied with prefixes.
   */
  public OccurrenceFeatures(Row row, String prefix, String... jsonFields) {
    this.row = row;
    this.prefix = prefix == null ? "" : prefix;
    this.jsonFields = jsonFields == null ? new HashSet() : new HashSet(Arrays.asList(jsonFields));
  }

  public OccurrenceFeatures(Row row, String prefix) {
    this(row, prefix, null);
  }

  public OccurrenceFeatures(Row row) {
    this(row, "");
  }

  <T> T get(String field) {
    int fieldIndex = assertNotNull(row.fieldIndex(prefix + field), "Field not found in row schema: "
      + prefix + field);
    return row.isNullAt(fieldIndex) ? null : row.getAs(fieldIndex);
  }

  Long getLong(String field) { return get(field); }

  String getString(String field) { return get(field); }

  Integer getInt(String field) { return get(field); }

  Double getDouble(String field) { return get(field); }
  List<String> getStrings(String... field) {
    List<String> vals = new ArrayList<>(field.length);
    Arrays.stream(field).forEach(s -> vals.add(getString(s)));
    return vals;
  }

  private static <T> T assertNotNull(T value, String message) {
    if (value == null) throw new IllegalArgumentException(message);
    else return value;
  }

  /**
   * @return JSON representing all fields that match the prefix, but with the prefix removed (e.g. t1_day becomes day)
   */
  public String asJson() throws IOException {
    // use the in built schema to extract all fields matching the prefix
    String[] fieldNames = row.schema().fieldNames();
    List<String> filteredFieldNames = Arrays.asList(fieldNames).stream()
      .filter(s -> prefix == null || prefix.length()==0 || s.startsWith(prefix))
      .collect(Collectors.toList());

    Map<String, Object> test = new HashMap();
    for (String field : filteredFieldNames) {
      Object o = row.isNullAt(row.fieldIndex(field)) ? null : row.getAs(field);

      if (jsonFields.contains(field)) {
        // Hack: parse the JSON so it will not be String encoded
        JsonNode media = OBJECT_MAPPER.readTree(String.valueOf(o));
        test.put(field.replaceFirst(prefix, ""), media);
      } else {
        test.put(field.replaceFirst(prefix, ""), o);
      }
    }

    return OBJECT_MAPPER.writeValueAsString(test);
  }
}
