package org.gbif.occurrence.common.json;

import org.gbif.api.model.common.MediaObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to serialize and deserialize MediaObject instances from/to JSON.
 */
public class MediaSerDeserUtils {

  private static final Logger LOG = LoggerFactory.getLogger(MediaSerDeserUtils.class);
  private static final String SER_ERROR_MSG = "Unable to serialize media objects to JSON";
  private static final String DESER_ERROR_MSG = "Unable to deserialize String into media objects";

  private static final ObjectMapper MAPPER = new ObjectMapper();
  static {
    // Don't change this section, methods used here guarantee backwards compatibility with Jackson 1.8.8
    MAPPER.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    MAPPER.configure(SerializationFeature.INDENT_OUTPUT, true);
    MAPPER.setSerializationInclusion(JsonInclude.Include.ALWAYS);
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
    } catch (IOException e) {
      logAndRethrow(DESER_ERROR_MSG, e);
    }
    return null;
  }

  /**
   * Converts a byte[] into String and extracts the media types from JSON representation of it.
   */
  public static Set<String> extractMediaTypes(byte[] input) {
    return extractMediaTypes(new String(input, StandardCharsets.UTF_8));
  }

  /**
   * Extracts the media types of JSON String.
   */
  public static Set<String> extractMediaTypes(String jsonMedias) {
    List<MediaObject> medias = MediaSerDeserUtils.fromJson(jsonMedias);
    Set<String> mediaTypes = Sets.newHashSet();
    if (medias != null && !medias.isEmpty()) {
      for (MediaObject mediaObject : medias) {
        if (mediaObject.getType() != null) {
          mediaTypes.add(mediaObject.getType().name().toUpperCase());
        }
      }
    }
    return mediaTypes;
  }

  /**
   * Logs an error and re-throws the exception.
   */
  private static void logAndRethrow(String message, Throwable throwable) {
    LOG.error(message, throwable);
    Throwables.propagate(throwable);
  }


}
