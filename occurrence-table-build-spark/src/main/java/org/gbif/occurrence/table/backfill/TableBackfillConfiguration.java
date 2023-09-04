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
package org.gbif.occurrence.table.backfill;

import java.io.File;
import java.io.IOException;

import org.yaml.snakeyaml.parser.ParserException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

@Data
@Builder
@Slf4j
@Jacksonized
public class TableBackfillConfiguration {

  private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

  static {
    MAPPER.setAnnotationIntrospector(
      new JacksonAnnotationIntrospector() {
        @Override
        public JsonPOJOBuilder.Value findPOJOBuilderConfig(AnnotatedClass ac) {
          if (ac.hasAnnotation(
            JsonPOJOBuilder.class)) { // If no annotation present use default as empty prefix
            return super.findPOJOBuilderConfig(ac);
          }
          return new JsonPOJOBuilder.Value("build", "");
        }
      });
  }

  private final HdfsLockConfiguration hdfsLock;

  @Nullable
  private Integer tablePartitions;

  private final String sourceDirectory;

  private final boolean usePartitionedTable;

  private final String targetDirectory;

  private final String coreName;

  private final String hiveDatabase;

  private final String tableName;

  private final String warehouseLocation;

  @Data
  @Builder
  @Jacksonized
  public static class HdfsLockConfiguration {

    private final String zkConnectionString;
    private final String namespace;
    private final String path;
    private final String name;

    @Builder.Default
    private final Integer connectionSleepTimeMs = 100;

    @Builder.Default
    private final Integer connectionMaxRetries = 5;

  }

  public static TableBackfillConfiguration loadFromFile(String fileName) {
    File file = new File(fileName);
    if (!file.exists()) {
      String message =
        "Error reading configuration file [" + fileName + "] because it does not exist";
      log.error(message);
      throw new IllegalArgumentException(message);
    }
    try {
        // read from YAML
        return MAPPER.readValue(file, TableBackfillConfiguration.class);
    } catch (IOException | ParserException e) {
      String message = "Error reading configuration file [" + fileName + "]";
      log.error(message);
      throw new IllegalArgumentException(message, e);
    }
  }
}
