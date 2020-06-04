package org.gbif.occurrence.common.json;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.CollectionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

/**
 * Utility class to serialize and deserialize Strings instances from/to JSON.
 */
public class ListStringSerDeserUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ListStringSerDeserUtils.class);
  private static final String SER_ERROR_MSG = "Unable to serialize list of string objects to JSON";
  private static final String DESER_ERROR_MSG = "Unable to deserialize String into list of string objects";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    // Don't change this section, methods used here guarantee backwards compatibility with Jackson 1.8.8
    MAPPER.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    MAPPER.configure(SerializationFeature.INDENT_OUTPUT, true);
    MAPPER.setSerializationInclusion(JsonInclude.Include.ALWAYS);
  }

  private static final CollectionType LIST_STRINGS_TYPE =
    MAPPER.getTypeFactory().constructCollectionType(List.class, String.class);


  private ListStringSerDeserUtils() {
    // private constructor
  }

  /**
   * Converts the list of string objects into a JSON string.
   */
  public static String toJson(List<String> strings) {
    try {
      if (strings != null && !strings.isEmpty()) {
        return MAPPER.writeValueAsString(strings);
      }
    } catch (IOException e) {
      logAndRethrow(SER_ERROR_MSG, e);
    }
    return "";
  }

  /**
   * Converts a Json string into a list of string objects.
   */
  public static List<String> fromJson(String strings) {
    try {
      return MAPPER.readValue(strings, LIST_STRINGS_TYPE);
    } catch (IOException e) {
      logAndRethrow(DESER_ERROR_MSG, e);
    }
    return null;
  }

  /**
   * Logs an error and re-throws the exception.
   */
  private static void logAndRethrow(String message, Throwable throwable) {
    LOG.error(message, throwable);
    Throwables.propagate(throwable);
  }


}
