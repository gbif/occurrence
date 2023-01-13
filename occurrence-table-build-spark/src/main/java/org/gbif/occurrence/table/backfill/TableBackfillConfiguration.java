package org.gbif.occurrence.table.backfill;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.parser.ParserException;

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

  private static final int DEFAULT_INDEXING_PARTITIONS = 80;

  private final HdfsLockConfiguration hdfsLock;

  private final String hdfsConfigFile;

  @Builder.Default
  private int tablePartitions = 200;

  private final String sourceDirectory;

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
