package org.gbif.occurrence.download.service.hive.jackson.serde;

import java.io.IOException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;
import org.gbif.occurrence.download.service.hive.validation.Query.Issue;


/**
 * 
 * Json serializer, to use issue name with description and comment upon serialization.
 *
 */
public class QueryIssueSerde extends JsonSerializer<Issue> {

  @Override
  public void serialize(Issue value, JsonGenerator generator, SerializerProvider provider) throws IOException, JsonProcessingException {
    generator.writeString(String.format("%s: %s %s", value.name(), value.description(), value.comment()));
  }
}
