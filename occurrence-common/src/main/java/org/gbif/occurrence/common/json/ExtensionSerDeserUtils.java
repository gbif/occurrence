/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.common.json;

import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Throwables;

/**
 * Utility class for common operations while persisting and retrieving extension objects.
 */
public class ExtensionSerDeserUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ExtensionSerDeserUtils.class);

  private static final String DEFAULT_ERROR_MSG = "Error serializing extensions values";

  private static final JavaType LIST_MAP_TERMS_TYPE;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
    MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
    MAPPER.setSerializationInclusion(JsonInclude.Include.ALWAYS);
    TypeFactory typeFactory = MAPPER.getTypeFactory();
    LIST_MAP_TERMS_TYPE =
      typeFactory.constructCollectionType(List.class,
        typeFactory.constructMapType(Map.class, Term.class, String.class));
    SimpleModule extensionsModule = new SimpleModule("Verbatim", Version.unknownVersion());
    extensionsModule.addSerializer(new InnerTermMapListSerializer());
    extensionsModule.addKeyDeserializer(Term.class, new KeyDeserializer() {
      private TermFactory factory = TermFactory.instance();
      @Override
      public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException {
        return factory.findTerm(key);
      }
    });
    MAPPER.registerModule(extensionsModule);

  }


  /**
   * Inner class that serializes List<Map<Term, String>> objects.
   */
  private static class InnerTermMapListSerializer extends StdSerializer<List<Map<Term, String>>> {

    protected InnerTermMapListSerializer() {
      super(LIST_MAP_TERMS_TYPE);
    }

    @Override
    public void serialize(List<Map<Term, String>> value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException {
      if ((value == null || value.isEmpty()) && provider.getConfig().isEnabled(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS)) {
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
    } catch (IOException  e) {
      LOG.error(DEFAULT_ERROR_MSG, e);
      Throwables.propagate(e);
    }
    return null;
  }


  /**
   * Deserializes a List<Map<Term, String>> from a Json String.
   */
  public static List<Map<Term, String>> fromJson(String json) {
    try (StringReader stringReader = new StringReader(json)) {
      return MAPPER.readValue(stringReader, LIST_MAP_TERMS_TYPE);
    } catch (IOException e) {
      LOG.error(DEFAULT_ERROR_MSG, e);
      Throwables.propagate(e);
    }
    return null;
  }
}
