package org.gbif.occurrence.processor.interpreting.util;

import org.gbif.ws.mixin.Mixins;

import java.util.Map;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import com.google.inject.Singleton;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

/**
 * Provider that initializes the {@link org.codehaus.jackson.map.ObjectMapper} to not fail on unknown properties.
 * This class also exists in common-ws, but this one here is compatible with jackson 1.8 and 1.9 so we can use it
 * also in Hive UDFs (which see jackson 1.8).
 */
@Provider
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class ObjectMapperContextResolver implements ContextResolver<ObjectMapper> {

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    // determines whether encountering of unknown properties (ones that do not map to a property, and there is no
    // "any setter" or handler that can handle it) should result in a failure (throwing a JsonMappingException) or not.
    MAPPER.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // Enforce use of ISO-8601 format dates (http://wiki.fasterxml.com/JacksonFAQDateHandling)
    MAPPER.configure(SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS, false);

    // register predefined Mixins
    for (Map.Entry<Class<?>, Class<?>> classClassEntry : Mixins.getPredefinedMixins().entrySet()) {
      MAPPER.getSerializationConfig().addMixInAnnotations(classClassEntry.getKey(), classClassEntry.getValue());
      MAPPER.getDeserializationConfig().addMixInAnnotations(classClassEntry.getKey(), classClassEntry.getValue());
    }
  }

  @Override
  public ObjectMapper getContext(Class<?> type) {
    return MAPPER;
  }
}

