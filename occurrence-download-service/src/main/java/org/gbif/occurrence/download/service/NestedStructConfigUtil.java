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
package org.gbif.occurrence.download.service;

import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Parses the checklist nested struct configs, which map checklist keys to the names of their
 * nested struct columns.
 */
public class NestedStructConfigUtil {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final TypeReference<Map<String, String>> STRING_MAP = new TypeReference<>() {};

  /**
   * Private constructor.
   */
  private NestedStructConfigUtil() {
    //empty constructor
  }

  /** Parses a checklist nested struct config, failing on an invalid JSON or a non string value. */
  public static Map<String, String> parse(String json, String name) {
    try {
      return MAPPER.readValue(json, STRING_MAP);
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid " + name + " JSON format", e);
    }
  }
}
