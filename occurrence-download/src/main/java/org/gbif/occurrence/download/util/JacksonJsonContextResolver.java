package org.gbif.occurrence.download.util;

import java.util.Map;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.inject.Singleton;

/**
 * Provider that initializes the {@link ObjectMapper} to ignore {@code null} fields and unknown
 * properties.
 * Copied and modified from gbif-common-ws to be compatible with jackson 1.8.8 used in the cdh4 environment.
 */
@Provider
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class JacksonJsonContextResolver implements ContextResolver<ObjectMapper> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    // determines whether encountering of unknown properties (ones that do not map to a property, and there is no
    // "any setter" or handler that can handle it) should result in a failure (throwing a JsonMappingException) or not.
    MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    MAPPER.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  /**
   * Specific polymorphic serialization is supported by passing a map of mixIns into the class.
   *
   * @param mixIns class map
   */
  public static void addMixIns(Map<Class<?>, Class<?>> mixIns) {
    // handle polymorphic JSON
    for (Map.Entry<Class<?>, Class<?>> classClassEntry : mixIns.entrySet()) {
      MAPPER.addMixIn(classClassEntry.getKey(), classClassEntry.getValue());
    }
  }

  @Override
  public ObjectMapper getContext(Class<?> type) {
    return MAPPER;
  }
}
