package org.gbif.occurrence.cli.registry;

import javax.ws.rs.ext.ContextResolver;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

public class RegistryObjectMapperContextResolver implements ContextResolver<ObjectMapper> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Override
  public ObjectMapper getContext(Class<?> type) {
    return MAPPER;
  }
}
