package org.gbif.occurrence.cli.registry;

import java.util.Map;
import javax.ws.rs.ext.ContextResolver;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * To investigate: Not sure why this class exists, JacksonJsonContextResolver is doing very similar.
 */
public class RegistryObjectMapperContextResolver implements ContextResolver<ObjectMapper> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  /**
   * Add MixIns to the ObjectMapper
   *
   * @param mixIns class map
   */
  public static void addMixIns(Map<Class<?>, Class<?>> mixIns) {
    for (Map.Entry<Class<?>, Class<?>> classClassEntry : mixIns.entrySet()) {
      MAPPER.getSerializationConfig().addMixInAnnotations(classClassEntry.getKey(), classClassEntry.getValue());
      MAPPER.getDeserializationConfig().addMixInAnnotations(classClassEntry.getKey(), classClassEntry.getValue());
    }
  }
  @Override
  public ObjectMapper getContext(Class<?> type) {
    return MAPPER;
  }
}
