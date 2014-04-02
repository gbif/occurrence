package org.gbif.occurrence.common.json;

import org.gbif.api.model.common.MediaObject;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Throwables;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.map.type.CollectionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MediaSerDeserUtils {

  private static final Logger LOG = LoggerFactory.getLogger(MediaSerDeserUtils.class);
  private static final String SER_ERROR_MSG = "Unable to serialize media objects to JSON";
  private static final String DESER_ERROR_MSG = "Unable to deserialize String into media objects";

  private static final ObjectMapper MAPPER = new ObjectMapper();
  static {
    // Don't change this section, methods used here guarantee backwards compatibility with Jackson 1.8.8
    MAPPER.configure(DeserializationConfig.Feature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    MAPPER.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
    MAPPER.getSerializationConfig().setSerializationInclusion(JsonSerialize.Inclusion.ALWAYS);
  }

  private static final CollectionType LIST_MEDIA_TYPE = MAPPER.getTypeFactory().constructCollectionType(List.class,
    MediaObject.class);


  private MediaSerDeserUtils() {
    // private constructor
  }

  /**
   * Converts the list of media objects into a JSON string.
   */
  public static String toJson(List<MediaObject> media) {
    try {
      if (media != null && !media.isEmpty()) {
        return MAPPER.writeValueAsString(media);
      }
    } catch (JsonGenerationException e) {
      logAndRethrow(SER_ERROR_MSG, e);
    } catch (JsonMappingException e) {
      logAndRethrow(SER_ERROR_MSG, e);
    } catch (IOException e) {
      logAndRethrow(SER_ERROR_MSG, e);
    }
    return null;
  }

  /**
   * Converts a Json string into a list of media objects.
   */
  public static List<MediaObject> fromJson(String mediaJson) {
    try {
      return MAPPER.readValue(mediaJson, LIST_MEDIA_TYPE);
    } catch (JsonParseException e) {
      logAndRethrow(DESER_ERROR_MSG, e);
    } catch (JsonMappingException e) {
      logAndRethrow(DESER_ERROR_MSG, e);
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
