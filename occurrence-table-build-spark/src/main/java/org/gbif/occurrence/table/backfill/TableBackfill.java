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

import org.apache.avro.Schema;
import org.gbif.occurrence.download.hive.ExtensionTable;
import org.gbif.occurrence.download.hive.InitializableField;
import org.gbif.occurrence.download.hive.OccurrenceAvroHdfsTableDefinition;
import org.gbif.occurrence.download.hive.OccurrenceHDFSTableDefinition;
import org.gbif.occurrence.spark.udf.UDFS;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructType;

import com.google.common.base.Strings;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.from_json;

@AllArgsConstructor
@Slf4j
public class TableBackfill {
  private final TableBackfillConfiguration configuration;

  private final String jobId = UUID.randomUUID().toString();

  public enum Option {
    TABLE,
    EXTENSIONS,
    MULTIMEDIA,
    ALL;
  }

  public enum Action {
    CREATE,
    DELETE,
    SCHEMA_MIGRATION;
  }

  @Data
  @Builder
  public static class Command {
    private final Action action;
    private final Set<Option> options;

    public static Command parse(String actionArg, String optionsArg) {
      Action action = Action.valueOf(actionArg.toUpperCase());
      Set<Option> options =
          Arrays.stream(optionsArg.split(","))
              .map(opt -> Option.valueOf(opt.toUpperCase()))
              .collect(Collectors.toSet());
      return Command.builder().action(action).options(options).build();
    }
  }

  public static void main(String[] args) {

    TableBackfillConfiguration configuration = TableBackfillConfiguration.builder()
      .prefixTable("dev")
      .tableName("occurrence")
      .usePartitionedTable(true)
      .build();
    TableBackfill backfill1 = new TableBackfill(configuration);
    String insertOverwrite = backfill1.insertOverWrite("occurrence", backfill1.occurrenceTableFields(), "occurrence_avro");
    System.out.println(insertOverwrite);

    // Read config file
    TableBackfillConfiguration tableBackfillConfiguration =
        TableBackfillConfiguration.loadFromFile(args[0]);

    TableBackfill backfill = new TableBackfill(tableBackfillConfiguration);

    backfill.run(Command.parse(args[1], args[2]));
  }

  private SparkSession createSparkSession() {
    // Removed the hive configs to see if it loads properly from the hive-site.xml
    //           .config("spark.sql.warehouse.dir", configuration.getWarehouseLocation())
    //           .config("hive.metastore.uris", configuration.getHiveThriftAddress())
    SparkSession.Builder sparkBuilder =
        SparkSession.builder()
            .appName(configuration.getTableName() + " table build")
            .enableHiveSupport()
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.defaultCatalog", "spark_catalog")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");

    if (configuration.getHiveThriftAddress() != null) {
      sparkBuilder
          .config("hive.metastore.uris", configuration.getHiveThriftAddress())
          .config("spark.sql.warehouse.dir", configuration.getWarehouseLocation());
    }
    return sparkBuilder.getOrCreate();
  }

  private void createTableUsingSpark(SparkSession spark) {
    // Create Hive Table if it doesn't exist
    spark.sql(createTableIfNotExists());
    fromAvroToTable(
        spark,
        getSnapshotPath(configuration.getCoreName()), // FROM
        configuration.getAvroTableName(),
        occurrenceTableFields(), // SELECT
        configuration.getTableNameWithPrefix(), // INSERT OVERWRITE INTO
        OccurrenceAvroHdfsTableDefinition.avroDefinition()
        );
  }

  private void executeCreateAction(Command command, SparkSession spark) {
    HdfsSnapshotAction snapshotAction =
        new HdfsSnapshotAction(configuration, spark.sparkContext().hadoopConfiguration());
    try {
      log.info("Using {} as snapshot name of source directory", jobId);
      snapshotAction.createHdfsSnapshot(jobId);
      UDFS.registerUdfs(spark);
      if (command.getOptions().contains(Option.ALL)
          || command.getOptions().contains(Option.TABLE)) {
        log.info("Creating Avro and Parquet Table for " + configuration.getTableName());
        createTableUsingSpark(spark);
      }

      if (command.getOptions().contains(Option.ALL)
          || command.getOptions().contains(Option.EXTENSIONS)) {
        log.info("Creating Extension tables");
        createExtensionTablesParallel(spark);
      }

      if (command.getOptions().contains(Option.ALL)
          || command.getOptions().contains(Option.MULTIMEDIA)) {
        log.info("Creating Multimedia Table");
        spark.sql(createIfNotExistsGbifMultimedia());
        insertOverwriteMultimediaTable(spark);
      }
    } finally {
      snapshotAction.deleteHdfsSnapshot(jobId);
      log.info("Creation finished");
    }
  }

  private void executeDeleteAction(Command command, SparkSession spark, String prefix) {
    if (command.getOptions().contains(Option.ALL) || command.getOptions().contains(Option.TABLE)) {
      log.info("Deleting Table " + configuration.getTableName());
      spark.sql(dropTable(prefix + configuration.getTableName()));
      spark.sql(dropTable(prefix + configuration.getTableName() + "_avro"));
    }
    if (command.getOptions().contains(Option.ALL)
        || command.getOptions().contains(Option.MULTIMEDIA)) {
      log.info("Deleting Multimedia Table ");
      spark.sql(dropTable(prefix + configuration.getTableName() + "_multimedia"));
    }
    if (command.getOptions().contains(Option.ALL)
        || command.getOptions().contains(Option.EXTENSIONS)) {
      log.info("Deleting Extension Tables");
      ExtensionTable.tableExtensions()
          .forEach(
              extensionTable -> {
                String extensionTableName = extensionTableName(extensionTable);
                log.info("Deleting Extension Table {}", extensionTableName);
                spark.sql(dropTable(prefix + extensionTableName));
              });
    }
  }

  public void run(Command command) {
    try (SparkSession spark = createSparkSession()) {
      spark.sql("USE " + configuration.getHiveDatabase());
      log.info("Running command " + command);
      if (Action.CREATE == command.getAction()) {
        executeCreateAction(command, spark);
      } else if (Action.SCHEMA_MIGRATION == command.getAction()) {
        // first we remove the old tables
        executeDeleteAction(command, spark, "old_");
        if (Strings.isNullOrEmpty(configuration.getPrefixTable())) {
          configuration.setPrefixTable("new");
        }
        executeCreateAction(command, spark);
        swapTables(command, spark);
      } else if (Action.DELETE == command.getAction()) {
        executeDeleteAction(command, spark, "");
      }
    }
  }

  @SneakyThrows
  private void createExtensionTablesParallel(SparkSession spark) {
    CountDownLatch doneSignal = new CountDownLatch(ExtensionTable.tableExtensions().size());
    ExecutorService executor =
        Executors.newFixedThreadPool(ExtensionTable.tableExtensions().size());
    ExtensionTable.tableExtensions()
        .forEach(
            extensionTable ->
                executor.submit(
                    new Runnable() {
                      @Override
                      public void run() {
                        createExtensionTable(spark, extensionTable);
                        doneSignal.countDown();
                      }
                    }));
    doneSignal.await();
    executor.shutdown();
  }

  @SneakyThrows
  private static boolean isDirectoryEmpty(String fromSourceDir, SparkSession spark) {
    return FileSystem.get(spark.sparkContext().hadoopConfiguration())
            .getContentSummary(new Path(fromSourceDir))
            .getFileCount()
        == 0;
  }

  private void fromAvroToTable(
      SparkSession spark, String fromSourceDir, String avroTableName, String selectFields, String saveToTable, Schema schema) {
    if (!isDirectoryEmpty(fromSourceDir, spark)) {
      if (configuration.isUsePartitionedTable()) {
        spark.sql(" set hive.exec.dynamic.partition.mode=nonstrict");
      }
      createSourceAvroTable(spark, avroTableName, schema,  fromSourceDir);
      String insertStatement = insertOverWrite(saveToTable, selectFields, avroTableName);
      System.out.println("Insert statement " + insertStatement);
      spark.sql(insertStatement);
    }
  }

  private String occurrenceTableFields() {
    return OccurrenceHDFSTableDefinition.definition().stream()
      // Excluding partitioned columns
      .filter(field -> configuration.isUsePartitionedTable() && (configuration.getDatasetKey() != null && !field.getHiveField().equalsIgnoreCase("datasetkey")) || configuration.getDatasetKey() == null)
      .map(InitializableField::getInitializer)
      .collect(Collectors.joining(", "));
  }
  private void createSourceAvroTable(SparkSession spark, String tableName, Schema schema, String location) {
    // Create Hive Table if it doesn't exist
    spark.sql("DROP TABLE IF EXISTS " + tableName);
    spark.sql("CREATE EXTERNAL TABLE " + tableName + " USING AVRO " +
      "OPTIONS( 'format' = 'avro', 'schema' = '" + schema.toString(true) + "') " +
      "LOCATION '" + location +  "' TBLPROPERTIES('iceberg.catalog'='location_based_table')");
  }

  private String insertOverWrite(String targetTableName, String selectFields, String sourceTable) {
    return "INSERT OVERWRITE TABLE " + targetTableName +
      (!Strings.isNullOrEmpty(configuration.getDatasetKey())? " PARTITION (datasetkey = '" + configuration.getDatasetKey() + "') " : " ") +
      "SELECT " + selectFields + " FROM " + sourceTable;
  }

  private void createExtensionTable(SparkSession spark, ExtensionTable extensionTable) {
    spark.sql(
        configuration.isUsePartitionedTable()
            ? createExtensionExternalTable(extensionTable)
            : createExtensionTable(extensionTable));

    String select =
        extensionTable.getFields().stream()
            .filter(
                field ->
                    !configuration.isUsePartitionedTable()
                        || !field.equalsIgnoreCase("datasetkey")) // Excluding partitioned columns
            .map(
                field ->
                    field.contains(")")
                        ? callUDF(
                                field.substring(0, field.indexOf('(')),
                                col(
                                    field.substring(
                                        field.indexOf('(') + 1, field.lastIndexOf(')'))))
                            .alias(field.substring(field.indexOf('(') + 1, field.lastIndexOf(')'))).toString()
                        : col(field).toString())
            .collect(Collectors.joining(","));

    fromAvroToTable(
        spark,
        getSnapshotPath(extensionTable.getDirectoryTableName()), // FROM sourceDir
        extensionAvroTableName(extensionTable),
        select, // SELECT
        extensionTableName(extensionTable),
        extensionTable.getSchema()); // INSERT OVERWRITE INTO
  }

  private String extensionTableName(ExtensionTable extensionTable) {
    return configuration.getTableName() + "_ext_" + extensionTable.getHiveTableName();
  }

  private String extensionAvroTableName(ExtensionTable extensionTable) {
    return configuration.getTableName() + "_ext" + extensionTable.getHiveTableName() + "_avro";
  }

  private String getPrefix() {
    return !Strings.isNullOrEmpty(configuration.getPrefixTable())
        ? configuration.getPrefixTable() + "_"
        : "";
  }

  private String createExtensionTable(ExtensionTable extensionTable) {
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s\n"
            + '('
            + extensionTable.getSchema().getFields().stream()
                .map(f -> f.name() + " STRING")
                .collect(Collectors.joining(",\n"))
            + ')'
            + "STORED AS PARQUET TBLPROPERTIES ('parquet.compression'='GZIP')\n",
        getPrefix() + extensionTableName(extensionTable));
  }

  private String createExtensionExternalTable(ExtensionTable extensionTable) {
    return String.format(
        "CREATE EXTERNAL TABLE IF NOT EXISTS %s\n"
            + '('
            + extensionTable.getSchema().getFields().stream()
                .filter(f -> !f.name().equalsIgnoreCase("datasetkey"))
                .map(f -> f.name() + " STRING")
                .collect(Collectors.joining(",\n"))
            + ')'
            + "PARTITIONED BY(datasetkey STRING) "
            + "TBLPROPERTIES ('iceberg.catalog'='location_based_table')\n",
        getPrefix() + extensionTableName(extensionTable),
        Paths.get(configuration.getTargetDirectory(), extensionTable.getHiveTableName()));
  }

  private String dropTable(String tableName) {
    return String.format("DROP TABLE IF EXISTS %s", tableName);
  }

  private String getSnapshotPath(String dataDirectory) {
    String path =
        Paths.get(
                configuration.getSourceDirectory(),
                configuration.getCoreName().toLowerCase(),
                ".snapshot",
                jobId,
                dataDirectory.toLowerCase())
            .toString();
    log.info("Snapshot path {}", path);
    return path;
  }

  public String createIfNotExistsGbifMultimedia() {
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s\n"
      + "(gbifid STRING, type STRING, format STRING, identifier STRING, references STRING, title STRING, description STRING,\n"
      + "source STRING, audience STRING, created STRING, creator STRING, contributor STRING,\n"
      + "publisher STRING, license STRING, rightsHolder STRING) USING iceberg \n"
      + "PARTITIONED BY(datasetkey STRING) "
      + "STORED AS PARQUET TBLPROPERTIES ('parquet.compression'='GZIP')",
        getPrefix() + multimediaTableName());
  }

  private String multimediaTableName() {
    return String.format("%s_multimedia", configuration.getTableName());
  }

  public void insertOverwriteMultimediaTable(SparkSession spark) {
    Dataset<Row> mmRecords = spark
                              .table(configuration.getTableName())
                              .select(
                                  col("gbifid"),
                                  from_json(
                                          col("ext_multimedia"),
                                          new ArrayType(
                                              new StructType()
                                                  .add("type", "string", false)
                                                  .add("format", "string", false)
                                                  .add("identifier", "string", false)
                                                  .add("references", "string", false)
                                                  .add("title", "string", false)
                                                  .add("description", "string", false)
                                                  .add("source", "string", false)
                                                  .add("audience", "string", false)
                                                  .add("created", "string", false)
                                                  .add("creator", "string", false)
                                                  .add("contributor", "string", false)
                                                  .add("publisher", "string", false)
                                                  .add("license", "string", false)
                                                  .add("rightsHolder", "string", false),
                                              true))
                                      .alias("mm_record"))
                              .select(col("gbifid"), explode(col("mm_record")).alias("mm_record"))
                              .select(
                                  col("gbifid"),
                                  callUDF("cleanDelimiters", col("mm_record.type")).alias("type"),
                                  callUDF("cleanDelimiters", col("mm_record.format")).alias("format"),
                                  callUDF("cleanDelimiters", col("mm_record.identifier")).alias("identifier"),
                                  callUDF("cleanDelimiters", col("mm_record.references")).alias("references"),
                                  callUDF("cleanDelimiters", col("mm_record.title")).alias("title"),
                                  callUDF("cleanDelimiters", col("mm_record.description")).alias("description"),
                                  callUDF("cleanDelimiters", col("mm_record.source")).alias("source"),
                                  callUDF("cleanDelimiters", col("mm_record.audience")).alias("audience"),
                                  col("mm_record.created").alias("created"),
                                  callUDF("cleanDelimiters", col("mm_record.creator")).alias("creator"),
                                  callUDF("cleanDelimiters", col("mm_record.contributor")).alias("contributor"),
                                  callUDF("cleanDelimiters", col("mm_record.publisher")).alias("publisher"),
                                  callUDF("cleanDelimiters", col("mm_record.license")).alias("license"),
                                  callUDF("cleanDelimiters", col("mm_record.rightsHolder")).alias("rightsHolder"));
       if (configuration.getDatasetKey() != null) {
          mmRecords = mmRecords.where("datasetkey = " + configuration.getDatasetKey());
       }
       mmRecords.createOrReplaceTempView("mm_records");

    spark.sql("INSERT OVERWRITE TABLE " + configuration.getTableName() + "_multimedia \n" +
      (!Strings.isNullOrEmpty(configuration.getDatasetKey())? " PARTITION (datasetkey = '" + configuration.getDatasetKey() + "') " : " ") +
      "SELECT gbifid, type, format, identifier, references, title, description, source, audience, created, creator, contributor, publisher, license, rightsHolder FROM mm_records");
  }

  private String createTableIfNotExists() {
    return configuration.isUsePartitionedTable()
        ? createPartitionedTableIfNotExists()
        : createParquetTableIfNotExists();
  }

  private String createParquetTableIfNotExists() {
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s (\n"
            + OccurrenceHDFSTableDefinition.definition().stream()
                .map(field -> field.getHiveField() + " " + field.getHiveDataType())
                .collect(Collectors.joining(", \n"))
            + ") STORED AS PARQUET TBLPROPERTIES ('parquet.compression'='SNAPPY')",
        configuration.getTableNameWithPrefix());
  }

  private String createPartitionedTableIfNotExists() {
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s ("
            + OccurrenceHDFSTableDefinition.definition().stream()
                .filter(
                    field ->
                        configuration.isUsePartitionedTable()
                            && !field
                                .getHiveField()
                                .equalsIgnoreCase("datasetkey")) // Excluding partitioned columns
                .map(field -> field.getHiveField() + " " + field.getHiveDataType())
                .collect(Collectors.joining(", "))
            + ") USING iceberg PARTITIONED BY(datasetkey STRING) "
            + "TBLPROPERTIES ('parquet.compression'='GZIP', 'auto.purge'='true')",
        configuration.getTableNameWithPrefix());
  }

  private void swapTables(Command command, SparkSession spark) {
    if (command.getOptions().contains(Option.ALL) || command.getOptions().contains(Option.TABLE)) {
      log.info("Swapping table: " + configuration.getTableName());
      spark.sql(renameTable(configuration.getTableName(), "old_" + configuration.getTableName()));
      spark.sql(renameTable(configuration.getTableNameWithPrefix(), configuration.getTableName()));
    }

    if (command.getOptions().contains(Option.ALL)
        || command.getOptions().contains(Option.EXTENSIONS)) {
      log.info("Swapping Extension tables");
      ExtensionTable.tableExtensions()
          .forEach(
              extensionTable -> {
                spark.sql(
                    renameTable(
                        extensionTableName(extensionTable),
                        "old_" + extensionTableName(extensionTable)));
                spark.sql(
                    renameTable(
                        getPrefix() + extensionTableName(extensionTable),
                        extensionTableName(extensionTable)));
              });
    }

    if (command.getOptions().contains(Option.ALL)
        || command.getOptions().contains(Option.MULTIMEDIA)) {
      log.info("Swapping Multimedia Table");
      spark.sql(renameTable(multimediaTableName(), "old_" + multimediaTableName()));
      spark.sql(renameTable(getPrefix() + multimediaTableName(), multimediaTableName()));
    }
  }

  private String renameTable(String oldTable, String newTable) {
    return String.format("ALTER TABLE %s RENAME TO %s", oldTable, newTable);
  }
}
