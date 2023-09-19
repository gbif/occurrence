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
package org.gbif.occurrence.download.hive;

import org.gbif.api.vocabulary.Extension;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.avro.Schema;

import com.google.common.collect.ImmutableMap;

import lombok.SneakyThrows;

/**
 * Utility class to reads the Avro schemas for extension tables.
 */
public class ExtensionSchemasLoader {

  private static String normalizeFileName(String name) {
    String result =
      Arrays.stream(name.split("_"))
        .map(String::toLowerCase)
        .collect(Collectors.joining("-"));
    return result + "-table.avsc";
  }

  /**
   * Reads the Avro schemas of extension tables.
   */
  @SneakyThrows
  private static void traverseTableSchemas(Consumer<Schema> schemaConsumer) {
    for (Extension extension : Extension.availableExtensions()) {
      String resourceName = "table/" + normalizeFileName(extension.name());
      try (InputStream schemaFile = ExtensionSchemasLoader.class.getClassLoader().getResourceAsStream(resourceName)) {
        Schema schema = new Schema.Parser().parse(schemaFile);
        schemaConsumer.accept(schema);
      }
    }
  }

  /**
   * Parses the Avro Schema name into a Extension enumeration.
   */
  private static Extension parseExtension(String name) {
    return Extension.valueOf(Arrays.stream(name.replace("Table", "").split("(?=[A-Z])"))
                               .map(String::toUpperCase)
                               .collect(Collectors.joining("_")));
  }

  /**
   * Creates a Map of Extension table names to their correspondent Extension enumeration.
   */
  @SneakyThrows
  public static Map<Extension, Schema> extensionTablesMap()  {
    ImmutableMap.Builder<Extension, Schema> extensionTables = ImmutableMap.builder();
    traverseTableSchemas(schema -> extensionTables.put(parseExtension(schema.getName()), schema));
    return extensionTables.build();
  }

}
