package org.gbif.occurrence.ws.provider;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;


/**
 * 
 * Providing default context provider for json producer in jax rs.
 *
 */
@Provider
@Produces(MediaType.APPLICATION_JSON)
public class JacksonContextResolver implements ContextResolver<ObjectMapper>{

  private ObjectMapper objectMapper;

  public JacksonContextResolver(){
      this.objectMapper = new ObjectMapper();
  this.objectMapper
      .configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
  }

  @Override
  public ObjectMapper getContext(Class<?> objectType) {
      return objectMapper;
  }

}
