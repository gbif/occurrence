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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import lombok.SneakyThrows;
import org.gbif.api.model.Constants;

import static org.gbif.occurrence.download.hive.OccurrenceAvroHdfsTableDefinition.avroField;

/**
 * Generates HQL scripts dynamically which are used to create the download HDFS tables, and querying when a user issues
 * a download request.
 * <p/>
 * Rather than generating HQL only at runtime, scripts are generated at build time using a Maven
 * plugin, to aid testing, development and debugging.  Freemarker is used as a templating language
 * to allow rapid development, but the sections which are verbose, and subject to easy typos are controlled
 * by enumerations in code.  The same enumerations are used in many places in the codebase, including the
 * generation of the Hive table columns themselves.
 */
public class GenerateHQL {

  private static final String CREATE_TABLES_DIR = "create-tables/hive-scripts";
  private static final String DOWNLOAD_DIR = "download-workflow/dwca/hive-scripts";
  private static final String SIMPLE_CSV_DOWNLOAD_DIR = "download-workflow/simple-csv/hive-scripts";
  private static final String SIMPLE_AVRO_DOWNLOAD_DIR = "download-workflow/simple-avro/hive-scripts";
  private static final String SIMPLE_PARQUET_DOWNLOAD_DIR = "download-workflow/simple-parquet/hive-scripts";
  private static final String SIMPLE_WITH_VERBATIM_AVRO_DOWNLOAD_DIR = "download-workflow/simple-with-verbatim-avro/hive-scripts";
  private static final String MAP_OF_LIFE_DOWNLOAD_DIR = "download-workflow/map-of-life/hive-scripts";

  private static final String BIONOMIA_DOWNLOAD_DIR = "download-workflow/bionomia/hive-scripts";

  private static final String AVRO_SCHEMAS_DIR = "create-tables/avro-schemas";

  private static final String FIELDS = "fields";

  private static final HiveQueries HIVE_QUERIES = new HiveQueries();
  private static final AvroQueries AVRO_QUERIES = new AvroQueries();
  private static final ParquetQueries PARQUET_QUERIES = new ParquetQueries();
  private static final ParquetSchemaQueries PARQUET_SCHEMA_QUERIES = new ParquetSchemaQueries();
  private static final AvroSchemaQueries AVRO_SCHEMA_QUERIES = new AvroSchemaQueries();
  private static final SimpleAvroSchemaQueries SIMPLE_AVRO_SCHEMA_QUERIES = new SimpleAvroSchemaQueries();

  public static void main(String[] args) {
    try {
      Preconditions.checkState(1 == args.length, "Output path for HQL files is required");
      File outDir = new File(args[0]);
      Preconditions.checkState(outDir.exists() && outDir.isDirectory(), "Output directory must exist");

      // create the subdirectories into which we will write
      File createTablesDir = new File(outDir, CREATE_TABLES_DIR);
      File downloadDir = new File(outDir, DOWNLOAD_DIR);
      File simpleCsvDownloadDir = new File(outDir, SIMPLE_CSV_DOWNLOAD_DIR);
      File simpleWithVerbatimAvroDownloadDir = new File(outDir, SIMPLE_WITH_VERBATIM_AVRO_DOWNLOAD_DIR);
      File simpleAvroDownloadDir = new File(outDir, SIMPLE_AVRO_DOWNLOAD_DIR);
      File simpleParquetDownloadDir = new File(outDir, SIMPLE_PARQUET_DOWNLOAD_DIR);
      File mapOfLifeDownloadDir = new File(outDir, MAP_OF_LIFE_DOWNLOAD_DIR);
      File avroSchemasDir = new File(outDir, AVRO_SCHEMAS_DIR);
      File bionomiaSchemasDir = new File(outDir, BIONOMIA_DOWNLOAD_DIR);

      createTablesDir.mkdirs();
      downloadDir.mkdirs();
      simpleCsvDownloadDir.mkdirs();
      simpleAvroDownloadDir.mkdirs();
      simpleParquetDownloadDir.mkdirs();
      simpleWithVerbatimAvroDownloadDir.mkdirs();
      mapOfLifeDownloadDir.mkdirs();
      avroSchemasDir.mkdirs();
      bionomiaSchemasDir.mkdirs();

      Configuration cfg = templateConfig();

      generateOccurrenceAvroSchema(avroSchemasDir);
      copyExtensionSchemas(avroSchemasDir);
      generateOccurrenceAvroTableHQL(cfg, createTablesDir);

      // generates HQL executed at actual download time (tightly coupled to table definitions above, hence this is
      // co-located)
      generateDwcaQueryHQL(cfg, Constants.NUB_DATASET_KEY.toString(), downloadDir);
      generateSimpleCsvQueryHQL(cfg, Constants.NUB_DATASET_KEY.toString(), simpleCsvDownloadDir);
      generateSimpleAvroQueryHQL(cfg, Constants.NUB_DATASET_KEY.toString(), simpleAvroDownloadDir);
      generateSimpleAvroSchema(cfg, Constants.NUB_DATASET_KEY.toString(), simpleAvroDownloadDir.getParentFile());
      generateSimpleParquetQueryHQL(cfg, Constants.NUB_DATASET_KEY.toString(), simpleParquetDownloadDir);
      generateSimpleWithVerbatimAvroQueryHQL(cfg, simpleWithVerbatimAvroDownloadDir);
      generateSimpleWithVerbatimAvroSchema(cfg, simpleWithVerbatimAvroDownloadDir.getParentFile());
      generateMapOfLifeQueryHQL(cfg, Constants.NUB_DATASET_KEY.toString(), mapOfLifeDownloadDir);
      generateMapOfLifeSchema(cfg, Constants.NUB_DATASET_KEY.toString(), mapOfLifeDownloadDir.getParentFile());
      generateBionomiaQueryHQL(cfg, bionomiaSchemasDir);

    } catch (Exception e) {
      // Hard exit for safety, and since this is used in build pipelines, any generation error could have
      // catastrophic effects - e.g. partially complete scripts being run, and resulting in inconsistent
      // data.
      System.err.println("*** Aborting JVM ***");
      System.err.println("Unexpected error building the templated HQL files.  "
                         + "Exiting JVM as a precaution, after dumping technical details.");
      e.printStackTrace();
      System.exit(-1);
    }

  }

  private static Configuration templateConfig() {
    Configuration cfg = new Configuration();
    cfg.setTemplateLoader(new ClassTemplateLoader(GenerateHQL.class, "/templates"));
    return cfg;
  }

  /**
   * Generates HQL which is used to create the Hive table, and creates an HDFS equivalent.
   */
  public static void generateOccurrenceAvroTableHQL(Configuration cfg, File outDir) throws IOException, TemplateException {

    try (FileWriter createTableScript = new FileWriter(new File(outDir, "create-occurrence-hive-tables.q"));
         FileWriter swapTablesScript = new FileWriter(new File(outDir, "swap-tables.q"));
         FileWriter dropExtensionsTablesScript = new FileWriter(new File(outDir, "drop-extension-tables.q"))) {
      Template createTableTemplate = cfg.getTemplate("create-tables/create-occurrence-hive-tables.ftl");
      Map<String, Object> data = ImmutableMap.of(FIELDS, OccurrenceHDFSTableDefinition.definition(),
                                                 "extensions", ExtensionTable.tableExtensions());
      createTableTemplate.process(data, createTableScript);

      Template swapTablesTemplate = cfg.getTemplate("create-tables/swap-tables.ftl");
      swapTablesTemplate.process(data, swapTablesScript);

      Template dropExtensionTablesTemplate = cfg.getTemplate("create-tables/drop-extension-tables.ftl");
      dropExtensionTablesTemplate.process(data, dropExtensionsTablesScript);
    }
  }

  private static void generateOccurrenceAvroSchema(File outDir) throws IOException {
    try (FileWriter out = new FileWriter(new File(outDir, "occurrence-hdfs-record.avsc"))) {
      out.write(OccurrenceAvroHdfsTableDefinition.avroDefinition().toString(Boolean.TRUE));
    }
  }

  private static void copyExtensionSchemas(File outDir) throws IOException {
    for (ExtensionTable et : ExtensionTable.tableExtensions()) {
      try (FileWriter out = new FileWriter(new File(outDir, et.getAvroSchemaFileName()))) {
        out.write(et.getSchema().toString(true));
      }
    }
  }

  /**
   * Generates the Hive query file used for DwCA downloads.
   */
  public static void generateDwcaQueryHQL(Configuration cfg, String checklistKey, File outDir) throws IOException, TemplateException {
    try (FileWriter out = new FileWriter(new File(outDir, "execute-query.q"))) {
      generateDwcaQueryHQL(cfg, checklistKey, out);
    }
    generateDwcaDropTableQueryHQL(cfg, outDir);
  }

  public static void generateDwcaQueryHQL(Configuration cfg, String checklistKey, Writer writer) throws IOException, TemplateException {
      Template template = cfg.getTemplate("download/execute-query.ftl");
      Map<String, Object> data = ImmutableMap.<String, Object>builder()
        .put("verbatimFields", HIVE_QUERIES.selectVerbatimFields().values())
        .put("interpretedFields", HIVE_QUERIES.selectInterpretedFields(false, checklistKey).values())
        .put("initializedInterpretedFields", HIVE_QUERIES.selectInterpretedFields(true, checklistKey).values())
        .put("multimediaFields", HIVE_QUERIES.selectMultimediaFields(false).values())
        .put("initializedMultimediaFields", HIVE_QUERIES.selectMultimediaFields(true).values())
        .put("extensions", ExtensionTable.tableExtensions())
        .build();
      template.process(data, writer);
  }

  public static void generateDwcaDropTableQueryHQL(Configuration cfg, File outDir) throws IOException, TemplateException {
    try (FileWriter out = new FileWriter(new File(outDir, "drop_tables.q"))) {
      Template template = cfg.getTemplate("download/drop_tables.ftl");
      Map<String, Object> data = Collections.singletonMap("extensions", ExtensionTable.tableExtensions());
      template.process(data, out);
    }
  }

  public static void generateDwcaDropTableQueryHQL(Writer writer) throws IOException, TemplateException {
      Template template = templateConfig().getTemplate("download/drop_tables.ftl");
      Map<String, Object> data = Collections.singletonMap("extensions", ExtensionTable.tableExtensions());
      template.process(data, writer);
  }

  /**
   * Generates the Hive query file used for CSV downloads.
   */
  public static void generateSimpleCsvQueryHQL(Configuration cfg, String checklistKey, File outDir) throws IOException, TemplateException {
    try (FileWriter out = new FileWriter(new File(outDir, "execute-simple-csv-query.q"))) {
      generateSimpleCsvQueryHQL(cfg, checklistKey, out);
    }
  }

  /**
   * Generates the Hive query file used for CSV downloads.
   */
  public static void generateSimpleCsvQueryHQL(Configuration cfg, String checklistKey, Writer writer) throws IOException, TemplateException {
    Template template = cfg.getTemplate("simple-csv-download/execute-simple-csv-query.ftl");
    Map<String, Object> data = ImmutableMap.of(
      FIELDS,
      HIVE_QUERIES.selectSimpleDownloadFields(true, checklistKey).values()
    );
    template.process(data, writer);
  }

  public static String generateDwcaQueryHQL(String checklistKey) throws IOException, TemplateException {
    try (StringWriter stringWriter = new StringWriter()) {
      generateDwcaQueryHQL(templateConfig(), checklistKey, stringWriter);
      return stringWriter.toString();
    }
  }

  /**
   * Generates the Hive query file used for CSV downloads.
   */
  public static String simpleCsvQueryHQL(String checklistKey) throws IOException, TemplateException {
    try (StringWriter stringWriter = new StringWriter()) {
      generateSimpleCsvQueryHQL(templateConfig(), checklistKey, stringWriter);
      return stringWriter.toString();
    }
  }

  @SneakyThrows
  public static String resourceAsString(String path) {
    try (InputStream in = GenerateHQL.class.getResourceAsStream(path);
         BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      return reader.lines().collect(Collectors.joining("\n"));
    }
  }

  @SneakyThrows
  public static String speciesListQueryHQL() {
    return resourceAsString("/download-workflow/species-list/hive-scripts/execute-species-list-query.q");
  }

  /**
   * Generates the schema file used for simple AVRO downloads.
   */
  public static void generateSimpleAvroSchema(Configuration cfg, String checklistKey, File outDir) throws IOException {
    try (FileWriter out = new FileWriter(new File(outDir, "simple-occurrence.avsc"))) {
      out.write(simpleAvroSchema(checklistKey).toString(true));
    }
  }

  public static Schema simpleAvroSchema(String checklistKey) throws IOException {
    Map<String, InitializableField> fields = SIMPLE_AVRO_SCHEMA_QUERIES.selectSimpleDownloadFields(true, checklistKey);

    SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder
      .record("SimpleOccurrence")
      .namespace("org.gbif.occurrence.download.avro").fields();
    fields.values().forEach(initializableField -> avroField(builder, initializableField));
    return builder.endRecord();
  }


  /**
   * Generates the Hive query file used for simple AVRO downloads.
   */
  private static void generateSimpleAvroQueryHQL(Configuration cfg, String checklistKey, File outDir) throws IOException, TemplateException {
    try (FileWriter out = new FileWriter(new File(outDir, "execute-simple-avro-query.q"))) {
      Template template = cfg.getTemplate("simple-avro-download/execute-simple-avro-query.ftl");
      Map<String, Object> data = ImmutableMap.of(FIELDS, AVRO_QUERIES.selectSimpleDownloadFields(true, checklistKey).values(),
                                             "avroSchema", simpleAvroSchema(checklistKey).toString(true));
      template.process(data, out);
    }
  }

  private static void generateSimpleAvroQueryHQL(Configuration cfg, String checklistKey, Writer out) throws IOException, TemplateException {
    Template template = cfg.getTemplate("simple-avro-download/execute-simple-avro-query.ftl");
    Map<String, Object> data = ImmutableMap.of(FIELDS, AVRO_QUERIES.selectSimpleDownloadFields(true, checklistKey).values(),
                                            "avroSchema", simpleAvroSchema(checklistKey).toString(true));
    template.process(data, out);
  }

  @SneakyThrows
  public static String simpleAvroQueryHQL(String checklistKey) {
    try (StringWriter out = new StringWriter()) {
      generateSimpleAvroQueryHQL(templateConfig(), checklistKey, out);
      return out.toString();
    }
  }

  /**
   * Generates the Hive query file used for simple Parquet downloads.
   */
  public static void generateSimpleParquetQueryHQL(Configuration cfg, String checklistKey, File outDir) throws IOException, TemplateException {
    try (FileWriter out = new FileWriter(new File(outDir, "execute-simple-parquet-query.q"))) {
      generateSimpleParquetQueryHQL(cfg, checklistKey, out);
    }
  }

  private static void generateSimpleParquetQueryHQL(Configuration cfg, String checklistKey, Writer out) throws IOException, TemplateException {
    Template template = cfg.getTemplate("simple-parquet-download/execute-simple-parquet-query.ftl");

    // We need the initializers (toLocalISO8601(eventDate) etc) but also the API-matching column name (verbatimScientificName etc).
    Map<String, InitializableField> interpretedNames = PARQUET_QUERIES.selectSimpleDownloadFields(true, checklistKey);
    Map<String, InitializableField> columnNames = PARQUET_SCHEMA_QUERIES.selectSimpleDownloadFields(false, checklistKey);

    Map<String, Object> data = ImmutableMap.of(
      "hiveFields", interpretedNames,
      "parquetFields", columnNames
    );
    template.process(data, out);
  }

  @SneakyThrows
  public static String simpleParquetQueryHQL(String checklistKey) {
    try (StringWriter out = new StringWriter()) {
      generateSimpleParquetQueryHQL(templateConfig(), checklistKey, out);
      return out.toString();
    }
  }

  @SneakyThrows
  public static String bionomiaQueryHQL() {
    try (StringWriter out = new StringWriter()) {
      generateBionomiaQueryHQL(templateConfig(), out);
      return out.toString();
    }
  }

  public static void generateBionomiaQueryHQL(Configuration cfg, File outDir) throws IOException, TemplateException {
    try (FileWriter out = new FileWriter(new File(outDir, "execute-bionomia-query.q"))) {
      generateBionomiaQueryHQL(cfg, out);
    }
  }
  public static void generateBionomiaQueryHQL(Configuration cfg, Writer out) throws IOException, TemplateException {
    Template template = cfg.getTemplate("bionomia/execute-bionomia-query.ftl");

    Map<String, Object> data = ImmutableMap.of(
      "bionomiaAvroSchema", resourceAsString("/download-workflow/bionomia/bionomia.avsc"),
      "bionomiaAgentsAvroSchema", resourceAsString("/download-workflow/bionomia/bionomia-agents.avsc"),
      "bionomiaFamiliesAvroSchema", resourceAsString("/download-workflow/bionomia/bionomia-families.avsc"),
      "bionomiaIdentifiersAvroSchema", resourceAsString("/download-workflow/bionomia/bionomia-identifiers.avsc")
    );
    template.process(data, out);
  }

  /**
   * Generates the Hive query file used for simple with verbatim AVRO downloads.
   */
  public static void generateSimpleWithVerbatimAvroQueryHQL(Configuration cfg, File outDir) throws IOException, TemplateException {
    try (FileWriter out = new FileWriter(new File(outDir, "execute-simple-with-verbatim-avro-query.q"))) {
      generateSimpleWithVerbatimAvroQueryHQL(cfg, out);
    }
  }

  private static void generateSimpleWithVerbatimAvroQueryHQL(Configuration cfg, Writer out) throws IOException, TemplateException {
    Template template = cfg.getTemplate("simple-with-verbatim-avro-download/execute-simple-with-verbatim-avro-query.ftl");

    Map<String, InitializableField> simpleFields = AVRO_QUERIES.selectSimpleWithVerbatimDownloadFields(true);
    Map<String, InitializableField> verbatimFields = new TreeMap(AVRO_QUERIES.selectVerbatimFields());

    // Omit any verbatim fields present in the simple download.
    for (String field : simpleFields.keySet()) {
      verbatimFields.remove(field);
    }

    Map<String, Object> data = ImmutableMap.of(
      "simpleFields", simpleFields,
      "verbatimFields", verbatimFields,
      "avroSchema", simpleWithVerbatimAvroSchema().toString(true)
    );
    template.process(data, out);

  }

  @SneakyThrows
  public static String simpleWithVerbatimAvroQueryHQL() {
    try (StringWriter out = new StringWriter()) {
      generateSimpleWithVerbatimAvroQueryHQL(templateConfig(), out);
      return out.toString();
    }
  }


  public Map<String, InitializableField> simpleWithVerbatimAvroQueryFields() {
    Map<String, InitializableField> simpleFields = AVRO_QUERIES.selectSimpleWithVerbatimDownloadFields(true);
    Map<String, InitializableField> verbatimFields = new TreeMap(AVRO_QUERIES.selectVerbatimFields());

    // Omit any verbatim fields present in the simple download.
    for (String field : simpleFields.keySet()) {
      verbatimFields.remove(field);
    }

    return ImmutableMap.<String, InitializableField>builder().putAll(simpleFields).putAll(verbatimFields).build();
  }


  /**
   * Generates the schema used for simple with verbatim AVRO downloads.
   */
  public static void generateSimpleWithVerbatimAvroSchema(Configuration cfg, File outDir) throws IOException {
    try (FileWriter out = new FileWriter(new File(outDir, "simple-with-verbatim-occurrence.avsc"))) {
      Schema schema = simpleWithVerbatimAvroSchema();

      out.write(schema.toString(true));
    }
  }

  public static Schema simpleWithVerbatimAvroSchema() {
    Map<String, InitializableField> simpleFields = AVRO_SCHEMA_QUERIES.selectSimpleWithVerbatimDownloadFields(true);
    Map<String, InitializableField> verbatimFields = new TreeMap(AVRO_SCHEMA_QUERIES.selectVerbatimFields());

    // Omit any verbatim fields present in the simple download.
    for (String field : simpleFields.keySet()) {
      verbatimFields.remove(field);
    }

    SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder
      .record("SimpleWithVerbatimOccurrence")
      .namespace("org.gbif.occurrence.download.avro").fields();
    simpleFields.values().forEach(initializableField -> avroField(builder, initializableField));
    verbatimFields.values().forEach(initializableField -> avroField(builder, initializableField));
    return builder.endRecord();
  }

  /**
   * Generates the AVRO schema for Map Of Life's custom format downloads.
   */
  private static void generateMapOfLifeSchema(Configuration cfg, String checklistKey, File outDir) throws IOException {
    try (FileWriter out = new FileWriter(new File(outDir, "map-of-life.avsc"))) {
      out.write(mapOfLifeSchema(checklistKey).toString(true));
    }
  }

  /**
   * Generates the AVRO schema for Map Of Life's custom format downloads.
   */
  static Schema mapOfLifeSchema(String checklistKey) {
    Map<String, InitializableField> fields = SIMPLE_AVRO_SCHEMA_QUERIES.selectGroupedDownloadFields(
      MapOfLifeDownloadDefinition.MAP_OF_LIFE_DOWNLOAD_TERMS, true, checklistKey);

    SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder
      .record("MapOfLife")
      .namespace("org.gbif.occurrence.download.avro").fields();
    fields.values().forEach(initializableField -> avroField(builder, initializableField));
    return builder.endRecord();
  }

  /**
   * Generates the Hive query file used for Map Of Life's custom format downloads.
   */
  private static void generateMapOfLifeQueryHQL(Configuration cfg, String checklistKey, File outDir) throws IOException, TemplateException {
    //AVRO_QUERIES.selectVerbatimFields().keySet().stream().forEach(System.out::println);
    //AVRO_QUERIES.selectInterpretedFields(true).keySet().stream().forEach(System.out::println);
    //AVRO_QUERIES.selectInternalFields(true).keySet().stream().forEach(System.out::println);
    try (FileWriter out = new FileWriter(new File(outDir, "execute-map-of-life-query.q"))) {
      generateMapOfLifeQueryHQL(cfg, checklistKey, out);
    }
  }

  private static void generateMapOfLifeQueryHQL(Configuration cfg, String checklistKey, Writer out) throws IOException, TemplateException {
    Template template = cfg.getTemplate("map-of-life-download/execute-map-of-life-query.ftl");
    Map<String, Object> data = ImmutableMap.of(
      "fields", AVRO_QUERIES.selectGroupedDownloadFields(MapOfLifeDownloadDefinition.MAP_OF_LIFE_DOWNLOAD_TERMS, true, checklistKey),
      "mapOfLifeAvroSchema", mapOfLifeSchema(checklistKey).toString(true)
    );
    template.process(data, out);

  }

  @SneakyThrows
  public static String mapOfLifeQueryHQL(String checklistKey) {
    try (StringWriter out = new StringWriter()) {
      generateMapOfLifeQueryHQL(templateConfig(), checklistKey, out);
      return out.toString();
    }
  }

  @SneakyThrows
  public static String sqlQueryHQL() {
    return resourceAsString("/download-workflow/sql-tsv/hive-scripts/execute-simple-sql-query.q");
  }
}
