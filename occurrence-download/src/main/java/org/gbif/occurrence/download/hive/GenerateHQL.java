package org.gbif.occurrence.download.hive;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

/**
 * Generates HQL scripts dynamically which are used to create the download HDFS tables, and querying when a user issues
 * a download request.
 * <p/>
 * Rather than generating HQL only at runtime, scripts are generated at build time using a maven
 * plugin, to aid testing, development and debugging.  Freemarker is used as a templating language
 * to allow rapid development, but the sections which are verbose, and subject to easy typos are controlled
 * by enumerations in code.  The same enumerations are used in many places in the codebase, including the
 * generation of HBase table columns themselves.
 */
public class GenerateHQL {

  private static final String CREATE_TABLES_DIR = "create-tables/hive-scripts";
  private static final String DOWNLOAD_DIR = "download-workflow/dwca/hive-scripts";
  private static final String SIMPLE_CSV_DOWNLOAD_DIR = "download-workflow/simple-csv/hive-scripts";
  private static final String SIMPLE_AVRO_DOWNLOAD_DIR = "download-workflow/simple-avro/hive-scripts";
  private static final String SPECIES_LIST_DOWNLOAD_DIR = "download-workflow/species-list/hive-scripts";
  
  private static final String FIELDS = "fields";
  
  public static void main(String[] args) {
    try {
      Preconditions.checkState(1 == args.length, "Output path for HQL files is required");
      File outDir = new File(args[0]);
      Preconditions.checkState(outDir.exists() && outDir.isDirectory(), "Output directory must exist");

      // create the sub directories into which we will write
      File createTablesDir = new File(outDir, CREATE_TABLES_DIR);
      File downloadDir = new File(outDir, DOWNLOAD_DIR);
      File simpleCsvDownloadDir = new File(outDir, SIMPLE_CSV_DOWNLOAD_DIR);
      File simpleAvroDownloadDir = new File(outDir, SIMPLE_AVRO_DOWNLOAD_DIR);
      File speciesListDownloadDir = new File(outDir, SPECIES_LIST_DOWNLOAD_DIR);
      
      createTablesDir.mkdirs();
      downloadDir.mkdirs();
      simpleCsvDownloadDir.mkdirs();
      simpleAvroDownloadDir.mkdirs();
      speciesListDownloadDir.mkdirs();
      
      Configuration cfg = new Configuration();
      cfg.setTemplateLoader(new ClassTemplateLoader(GenerateHQL.class, "/templates"));

      // generates HQL for the coordinator jobs to create the tables to be queried
      generateHBaseTableHQL(cfg, createTablesDir);
      generateOccurrenceTableHQL(cfg, createTablesDir);

      // generates HQL executed at actual download time (tightly coupled to table definitions above, hence this is
      // co-located)
      generateQueryHQL(cfg, downloadDir);
      generateSimpleCsvQueryHQL(cfg, simpleCsvDownloadDir);
      generateSimpleAvroQueryHQL(cfg, simpleAvroDownloadDir);
      generateSpeciesListQueryHQL(cfg, speciesListDownloadDir);

    } catch (Exception e) {
      // Hard exit for safety, and since this is used in build pipelines, any generation error could have
      // catastophic effects - e.g. partially complete scripts being run, and resulting in inconsistent
      // data.
      System.err.println("*** Aborting JVM ***");
      System.err.println("Unexpected error building the templated HQL files.  "
                         + "Exiting JVM as a precaution, after dumping technical details.");
      e.printStackTrace();
      System.exit(-1);
    }

  }

  /**
   * Generates HQL which create a Hive table on the HBase table.
   */
  private static void generateHBaseTableHQL(Configuration cfg, File outDir) throws IOException, TemplateException {
    try (FileWriter out = new FileWriter(new File(outDir, "create-occurrence-hbase.q"))) {
      Template template = cfg.getTemplate("configure/create-occurrence-hbase.ftl");
      Map<String, Object> data = ImmutableMap.<String, Object>of(FIELDS, OccurrenceHBaseTableDefinition.definition());
      template.process(data, out);
    }
  }

  /**
   * Generates HQL which is used to take snapshots of the HBase table, and creates an HDFS equivalent.
   */
  private static void generateOccurrenceTableHQL(Configuration cfg, File outDir) throws IOException, TemplateException {

    try (FileWriter out = new FileWriter(new File(outDir, "create-occurrence-hdfs.q"))) {
      Template template = cfg.getTemplate("configure/create-occurrence-hdfs.ftl");
      Map<String, Object> data = ImmutableMap.<String, Object>of(FIELDS, OccurrenceHDFSTableDefinition.definition());
      template.process(data, out);
    }
  }

  /**
   * Generates the Hive query file used for DwAc downloads.
   */
  private static void generateQueryHQL(Configuration cfg, File outDir) throws IOException, TemplateException {
    try (FileWriter out = new FileWriter(new File(outDir, "execute-query.q"))) {
      Template template = cfg.getTemplate("download/execute-query.ftl");
      Map<String, Object> data = ImmutableMap.<String, Object>of("verbatimFields",
                                                                 Queries.selectVerbatimFields(),
                                                                 "interpretedFields",
                                                                 Queries.selectInterpretedFields(false),
                                                                 "initializedInterpretedFields",
                                                                 Queries.selectInterpretedFields(true));
      template.process(data, out);
    }
  }

  /**
   * Generates the Hive query file used for CSV downloads.
   */
  private static void generateSimpleCsvQueryHQL(Configuration cfg, File outDir) throws IOException, TemplateException {
    try (FileWriter out = new FileWriter(new File(outDir, "execute-simple-csv-query.q"))) {
      Template template = cfg.getTemplate("simple-csv-download/execute-simple-csv-query.ftl");
      Map<String, Object> data = ImmutableMap.<String, Object>of(FIELDS, Queries.selectSimpleDownloadFields());
      template.process(data, out);
    }
  }
  
  /**
   * Generates the Hive query file used for Species list downloads.
   */
  private static void generateSpeciesListQueryHQL(Configuration cfg, File outDir) throws IOException, TemplateException {
    try (FileWriter out = new FileWriter(new File(outDir, "execute-species-list-query.q"))) {
      Template template = cfg.getTemplate("species-list-download/execute-species-list-query.ftl");
      Map<String, Object> data = ImmutableMap.<String, Object>of(FIELDS, Queries.selectSimpleDownloadFields());
      template.process(data, out);
    }
  }

  /**
   * Generates the Hive query file used for AVRO downloads.
   */
  private static void generateSimpleAvroQueryHQL(Configuration cfg, File outDir) throws IOException, TemplateException {
    try (FileWriter out = new FileWriter(new File(outDir, "execute-simple-avro-query.q"))) {
      Template template = cfg.getTemplate("simple-avro-download/execute-simple-avro-query.ftl");
      Map<String, Object> data = ImmutableMap.<String, Object>of(FIELDS, Queries.selectSimpleDownloadFields());
      template.process(data, out);
    }
  }
}
