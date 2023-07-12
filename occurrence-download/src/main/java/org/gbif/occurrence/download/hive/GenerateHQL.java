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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

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
  private static final String IUCN_DOWNLOAD_DIR = "download-workflow/iucn/hive-scripts";
  private static final String MAP_OF_LIFE_DOWNLOAD_DIR = "download-workflow/map-of-life/hive-scripts";
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
      File iucnDownloadDir = new File(outDir, IUCN_DOWNLOAD_DIR);
      File mapOfLifeDownloadDir = new File(outDir, MAP_OF_LIFE_DOWNLOAD_DIR);
      File avroSchemasDir = new File(outDir, AVRO_SCHEMAS_DIR);

      createTablesDir.mkdirs();
      downloadDir.mkdirs();
      simpleCsvDownloadDir.mkdirs();
      simpleAvroDownloadDir.mkdirs();
      simpleParquetDownloadDir.mkdirs();
      simpleWithVerbatimAvroDownloadDir.mkdirs();
      iucnDownloadDir.mkdirs();
      mapOfLifeDownloadDir.mkdirs();
      avroSchemasDir.mkdirs();

      Configuration cfg = templateConfig();

      generateOccurrenceAvroSchema(avroSchemasDir);
      copyExtensionSchemas(avroSchemasDir);
      generateOccurrenceAvroTableHQL(cfg, createTablesDir);

      // generates HQL executed at actual download time (tightly coupled to table definitions above, hence this is
      // co-located)
      generateDwcaQueryHQL(cfg, downloadDir);
      generateSimpleCsvQueryHQL(cfg, simpleCsvDownloadDir);
      generateSimpleAvroQueryHQL(cfg, simpleAvroDownloadDir);
      generateSimpleAvroSchema(cfg, simpleAvroDownloadDir.getParentFile());
      generateSimpleParquetQueryHQL(cfg, simpleParquetDownloadDir);
      generateSimpleWithVerbatimAvroQueryHQL(cfg, simpleWithVerbatimAvroDownloadDir);
      generateSimpleWithVerbatimAvroSchema(cfg, simpleWithVerbatimAvroDownloadDir.getParentFile());
      generateIucnQueryHQL(cfg, iucnDownloadDir);
      generateMapOfLifeQueryHQL(cfg, mapOfLifeDownloadDir);
      generateMapOfLifeSchema(cfg, mapOfLifeDownloadDir.getParentFile());

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

    try (FileWriter createTableScript = new FileWriter(new File(outDir, "create-occurrence-avro.q"));
         FileWriter swapTablesScript = new FileWriter(new File(outDir, "swap-tables.q"));
         FileWriter dropExtensionsTablesScript = new FileWriter(new File(outDir, "drop-extension-tables.q"))) {
      Template createTableTemplate = cfg.getTemplate("create-tables/create-occurrence-avro.ftl");
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
  public static void generateDwcaQueryHQL(Configuration cfg, File outDir) throws IOException, TemplateException {
    try (FileWriter out = new FileWriter(new File(outDir, "execute-query.q"))) {
      generateDwcaQueryHQL(cfg, out);
    }
    generateDwcaDropTableQueryHQL(cfg, outDir);
  }

  public static void generateDwcaQueryHQL(Configuration cfg, Writer writer) throws IOException, TemplateException {
      Template template = cfg.getTemplate("download/execute-query.ftl");
      Map<String, Object> data = ImmutableMap.<String, Object>builder()
        .put("verbatimFields", HIVE_QUERIES.selectVerbatimFields().values())
        .put("interpretedFields", HIVE_QUERIES.selectInterpretedFields(false).values())
        .put("initializedInterpretedFields", HIVE_QUERIES.selectInterpretedFields(true).values())
        .put("multimediaFields", HIVE_QUERIES.selectMultimediaFields(false).values())
        .put("initializedMultimediaFields", HIVE_QUERIES.selectMultimediaFields(true).values())
        .put("extensions", ExtensionTable.tableExtensions())
        .build();
      template.process(data, writer);
  }

  public static void generateDwcaQueryHQL(Writer writer) throws IOException, TemplateException {
    generateDwcaQueryHQL(templateConfig(), writer);
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
  public static void generateSimpleCsvQueryHQL(Configuration cfg, File outDir) throws IOException, TemplateException {
    try (FileWriter out = new FileWriter(new File(outDir, "execute-simple-csv-query.q"))) {
      generateSimpleCsvQueryHQL(cfg, out);
    }
  }

  /**
   * Generates the Hive query file used for CSV downloads.
   */
  public static void generateSimpleCsvQueryHQL(Configuration cfg, Writer writer) throws IOException, TemplateException {
    Template template = cfg.getTemplate("simple-csv-download/execute-simple-csv-query.ftl");
    Map<String, Object> data = ImmutableMap.of(FIELDS, HIVE_QUERIES.selectSimpleDownloadFields(true).values());
    template.process(data, writer);
  }

  /**
   * Generates the Hive query file used for CSV downloads.
   */
  public static void generateSimpleCsvQueryHQL(Writer writer) throws IOException, TemplateException {
    Template template = templateConfig().getTemplate("simple-csv-download/execute-simple-csv-query.ftl");
    Map<String, Object> data = ImmutableMap.of(FIELDS, HIVE_QUERIES.selectSimpleDownloadFields(true).values());
    template.process(data, writer);
  }

  /**
   * Generates the schema file used for simple AVRO downloads.
   */
  public static void generateSimpleAvroSchema(Configuration cfg, File outDir) throws IOException {
    try (FileWriter out = new FileWriter(new File(outDir, "simple-occurrence.avsc"))) {
      out.write(simpleAvroSchema().toString(true));
    }
  }

  public static Schema simpleAvroSchema() throws IOException {
    Map<String, InitializableField> fields = SIMPLE_AVRO_SCHEMA_QUERIES.selectSimpleDownloadFields(true);

    SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder
      .record("SimpleOccurrence")
      .namespace("org.gbif.occurrence.download.avro").fields();
    fields.values().forEach(initializableField -> avroField(builder, initializableField));
    return builder.endRecord();
  }


  /**
   * Generates the Hive query file used for simple AVRO downloads.
   */
  private static void generateSimpleAvroQueryHQL(Configuration cfg, File outDir) throws IOException, TemplateException {
    try (FileWriter out = new FileWriter(new File(outDir, "execute-simple-avro-query.q"))) {
      Template template = cfg.getTemplate("simple-avro-download/execute-simple-avro-query.ftl");
      Map<String, Object> data = ImmutableMap.of(FIELDS, AVRO_QUERIES.selectSimpleDownloadFields(true).values());
      template.process(data, out);
    }
  }

  /**
   * Generates the Hive query file used for simple Parquet downloads.
   */
  public static void generateSimpleParquetQueryHQL(Configuration cfg, File outDir) throws IOException, TemplateException {
    try (FileWriter out = new FileWriter(new File(outDir, "execute-simple-parquet-query.q"))) {
      Template template = cfg.getTemplate("simple-parquet-download/execute-simple-parquet-query.ftl");

      // We need the initializers (toLocalISO8601(eventDate) etc) but also the API-matching column name (verbatimScientificName etc).
      Map<String, InitializableField> interpretedNames = PARQUET_QUERIES.selectSimpleDownloadFields(true);
      Map<String, InitializableField> columnNames = PARQUET_SCHEMA_QUERIES.selectSimpleDownloadFields(false);

      Map<String, Object> data = ImmutableMap.of(
        "hiveFields", interpretedNames,
        "parquetFields", columnNames
      );
      template.process(data, out);
    }
  }

  /**
   * Generates the Hive query file used for simple with verbatim AVRO downloads.
   */
  public static void generateSimpleWithVerbatimAvroQueryHQL(Configuration cfg, File outDir) throws IOException, TemplateException {
    try (FileWriter out = new FileWriter(new File(outDir, "execute-simple-with-verbatim-avro-query.q"))) {
      Template template = cfg.getTemplate("simple-with-verbatim-avro-download/execute-simple-with-verbatim-avro-query.ftl");

      Map<String, InitializableField> simpleFields = AVRO_QUERIES.selectSimpleWithVerbatimDownloadFields(true);
      Map<String, InitializableField> verbatimFields = new TreeMap(AVRO_QUERIES.selectVerbatimFields());

      // Omit any verbatim fields present in the simple download.
      for (String field : simpleFields.keySet()) {
        verbatimFields.remove(field);
      }

      Map<String, Object> data = ImmutableMap.of(
        "simpleFields", simpleFields,
        "verbatimFields", verbatimFields
      );
      template.process(data, out);
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
   * Generates the Hive query file used for IUCN's custom format downloads.
   */
  private static void generateIucnQueryHQL(Configuration cfg, File outDir) throws IOException, TemplateException {
    try (FileWriter out = new FileWriter(new File(outDir, "execute-iucn-query.q"))) {
      Template template = cfg.getTemplate("iucn-download/execute-iucn-query.ftl");
      Map<String, Object> data = ImmutableMap.of(
        "verbatimFields", AVRO_QUERIES.selectVerbatimFields(),
        "interpretedFields", AVRO_QUERIES.selectInterpretedFields(true),
        "internalFields", AVRO_QUERIES.selectInternalFields(true)
      );
      template.process(data, out);
    }
  }

  /**
   * Generates the AVRO schema for Map Of Life's custom format downloads.
   */
  private static void generateMapOfLifeSchema(Configuration cfg, File outDir) throws IOException {
    try (FileWriter out = new FileWriter(new File(outDir, "map-of-life.avsc"))) {
      out.write(mapOfLifeSchema().toString(true));
    }
  }

  /**
   * Generates the AVRO schema for Map Of Life's custom format downloads.
   */
  static Schema mapOfLifeSchema() throws IOException {
    Map<String, InitializableField> fields = SIMPLE_AVRO_SCHEMA_QUERIES.selectGroupedDownloadFields(MapOfLifeDownloadDefinition.MAP_OF_LIFE_DOWNLOAD_TERMS, true);

    SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder
      .record("MapOfLife")
      .namespace("org.gbif.occurrence.download.avro").fields();
    fields.values().forEach(initializableField -> avroField(builder, initializableField));
    return builder.endRecord();
  }

  /**
   * Generates the Hive query file used for Map Of Life's custom format downloads.
   */
  private static void generateMapOfLifeQueryHQL(Configuration cfg, File outDir) throws IOException, TemplateException {
    //AVRO_QUERIES.selectVerbatimFields().keySet().stream().forEach(System.out::println);
    //AVRO_QUERIES.selectInterpretedFields(true).keySet().stream().forEach(System.out::println);
    //AVRO_QUERIES.selectInternalFields(true).keySet().stream().forEach(System.out::println);
    try (FileWriter out = new FileWriter(new File(outDir, "execute-map-of-life-query.q"))) {
      Template template = cfg.getTemplate("map-of-life-download/execute-map-of-life-query.ftl");
      Map<String, Object> data = ImmutableMap.of(
        "fields", AVRO_QUERIES.selectGroupedDownloadFields(MapOfLifeDownloadDefinition.MAP_OF_LIFE_DOWNLOAD_TERMS, true)
      );
      template.process(data, out);
    }
  }
}
