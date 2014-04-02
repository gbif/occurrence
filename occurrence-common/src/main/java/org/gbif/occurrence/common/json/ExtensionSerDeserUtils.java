package org.gbif.occurrence.common.json;

import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.jackson.TermKeyDeserializer;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Throwables;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.SerializationConfig.Feature;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.map.ser.std.SerializerBase;
import org.codehaus.jackson.map.type.TypeFactory;
import org.codehaus.jackson.type.JavaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for common operations while persisting and retrieving extension objects.
 */
public class ExtensionSerDeserUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ExtensionSerDeserUtils.class);

  private static final String DEFAULT_ERROR_MSG = "Error serializing extensions values";

  private static final JavaType LIST_MAP_TERMS_TYPE;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.enable(DeserializationConfig.Feature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
    MAPPER.enable(SerializationConfig.Feature.INDENT_OUTPUT);
    MAPPER.setSerializationConfig(MAPPER.getSerializationConfig().withSerializationInclusion(Inclusion.ALWAYS));
    TypeFactory typeFactory = MAPPER.getTypeFactory();
    LIST_MAP_TERMS_TYPE =
      typeFactory.constructCollectionType(List.class,
        typeFactory.constructMapType(Map.class, Term.class, String.class));
    SimpleModule extensionsModule = new SimpleModule("Verbatim", Version.unknownVersion());
    extensionsModule.addSerializer(new InnerTermMapListSerializer());
    extensionsModule.addKeyDeserializer(Term.class, new TermKeyDeserializer());
    MAPPER.registerModule(extensionsModule);

  }


  /**
   * Inner class that serializes List<Map<Term, String>> objects.
   */
  private static class InnerTermMapListSerializer extends SerializerBase<List<Map<Term, String>>> {

    protected InnerTermMapListSerializer() {
      super(LIST_MAP_TERMS_TYPE);
    }

    @Override
    public void serialize(List<Map<Term, String>> value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException {
      if ((value == null || value.isEmpty()) && provider.getConfig().isEnabled(Feature.WRITE_EMPTY_JSON_ARRAYS)) {
        jgen.writeStartArray();
        jgen.writeEndArray();
      } else {
        jgen.writeStartArray();
        for (Map<Term, String> extension : value) {
          jgen.writeStartObject();
          for (Entry<Term, String> entry : extension.entrySet()) {
            jgen.writeStringField(entry.getKey().qualifiedName(), entry.getValue());
          }
          jgen.writeEndObject();
        }
        jgen.writeEndArray();
      }
    }
  }

  private ExtensionSerDeserUtils() {
    // private constructor
  }

  /**
   * Serializes a List<Map<Term, String>> to a Json String.
   */
  public static String toJson(List<Map<Term, String>> extensionValues) {
    try {
      return MAPPER.writeValueAsString(extensionValues);
    } catch (JsonGenerationException e) {
      LOG.error(DEFAULT_ERROR_MSG, e);
      Throwables.propagate(e);
    } catch (JsonMappingException e) {
      LOG.error(DEFAULT_ERROR_MSG, e);
      Throwables.propagate(e);
    } catch (IOException e) {
      LOG.error(DEFAULT_ERROR_MSG, e);
      Throwables.propagate(e);
    }
    return null;
  }


  /**
   * Deserializes a List<Map<Term, String>> from a Json String.
   */
  public static List<Map<Term, String>> fromJson(String json) {
    StringReader stringReader = new StringReader(json);
    try {
      return MAPPER.readValue(stringReader, LIST_MAP_TERMS_TYPE);
    } catch (JsonParseException e) {
      LOG.error(DEFAULT_ERROR_MSG, e);
      Throwables.propagate(e);
    } catch (JsonMappingException e) {
      LOG.error(DEFAULT_ERROR_MSG, e);
      Throwables.propagate(e);
    } catch (IOException e) {
      LOG.error(DEFAULT_ERROR_MSG, e);
      Throwables.propagate(e);
    } finally {
      stringReader.close();
    }
    return null;
  }
}
