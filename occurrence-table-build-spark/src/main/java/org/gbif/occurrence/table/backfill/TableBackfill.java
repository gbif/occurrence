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

import org.gbif.occurrence.download.hive.ExtensionTable;
import org.gbif.occurrence.download.hive.OccurrenceHDFSTableDefinition;
import org.gbif.occurrence.spark.udf.UDFS;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.google.common.base.Strings;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.lit;

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
            .config("spark.sql.catalog.iceberg.type", "hive")
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.defaultCatalog", "iceberg");

    if (configuration.getHiveThriftAddress() != null) {
      sparkBuilder
          .config("hive.metastore.uris", configuration.getHiveThriftAddress())
          .config("spark.sql.warehouse.dir", configuration.getWarehouseLocation());
    }

    if (configuration.isUsePartitionedTable()) {
      sparkBuilder.config("spark.sql.sources.partitionOverwriteMode", "dynamic");
    }
    return sparkBuilder.getOrCreate();
  }

  private void createTableUsingSpark(SparkSession spark) {
    log.info("Creating Avro and Parquet Table for " + configuration.getTableName());
    spark.sparkContext().setJobDescription("Create " + configuration.getTableNameWithPrefix());

    spark.sql(createTableIfNotExists());
    fromAvroToTable(
        spark,
        getSnapshotPath(configuration.getCoreName()), // FROM
        selectFromAvro(), // SELECT
        configuration.getTableNameWithPrefix() // INSERT OVERWRITE INTO
        );
  }

  private void createMultimediaTable(SparkSession spark) {
    log.info("Creating Multimedia Table");
    spark.sparkContext().setJobDescription("Create " + multimediaTableName());

    spark.sql(createIfNotExistsGbifMultimedia());
    insertOverwriteMultimediaTable(spark);
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
        createTableUsingSpark(spark);
      }

      if (command.getOptions().contains(Option.ALL)
          || command.getOptions().contains(Option.EXTENSIONS)) {
        createExtensionTables(spark);
      }

      if (command.getOptions().contains(Option.ALL)
          || command.getOptions().contains(Option.MULTIMEDIA)) {
        createMultimediaTable(spark);
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
  private void createExtensionTables(SparkSession spark) {
    log.info("Creating Extension tables");
    try {
      ExtensionTable.tableExtensions().forEach(table -> createExtensionTable(spark, table));
    } catch (Exception ex) {
      log.error("Error creating extension tables");
      throw  ex;
    }
  }

  @SneakyThrows
  private static boolean isDirectoryEmpty(String fromSourceDir, SparkSession spark) {
    return FileSystem.get(spark.sparkContext().hadoopConfiguration())
            .getContentSummary(new Path(fromSourceDir))
            .getFileCount()
        == 0;
  }

  private void fromAvroToTable(
      SparkSession spark, String fromSourceDir, Column[] select, String saveToTable) {
    if (!isDirectoryEmpty(fromSourceDir, spark)) {
      if (configuration.isUsePartitionedTable()) {
        spark.sql(" set hive.exec.dynamic.partition.mode=nonstrict");
      }
      Dataset<Row> input =
          spark.read().format("avro").load(fromSourceDir + "/*.avro").select(select);

      if (configuration.getTablePartitions() != null
          && input.rdd().getNumPartitions() > configuration.getTablePartitions()) {
        log.info("Setting partitions options {}", configuration.getTablePartitions());
        input =
            input
                .withColumn(
                    "_salted_key",
                    col("gbifid").cast(DataTypes.LongType).mod(configuration.getTablePartitions()))
                .repartition(configuration.getTablePartitions())
                .drop("_salted_key");
      }

      input.writeTo(saveToTable).createOrReplace();
    }
  }

  private void createExtensionTable(SparkSession spark, ExtensionTable extensionTable) {
    log.info("Create extansion table: {}", extensionTable.getHiveTableName());
    spark.sparkContext().setJobDescription("Create " + extensionTable.getHiveTableName());

    spark.sql(
        configuration.isUsePartitionedTable()
            ? createExtensionExternalTable(extensionTable)
            : createExtensionTable(extensionTable));

    List<Column> columns =
        extensionTable.getFieldNames().stream()
            .filter(
                field ->
                    !configuration.isUsePartitionedTable()
                        || !field.equalsIgnoreCase("datasetkey")) // Excluding partitioned columns
            .map(
                // add _ for names staring with a number
                f ->
                    f.charAt(0) == '`' && Character.isDigit(f.charAt(1))
                        ? "`_" + f.substring(1)
                        : f)
            .map(functions::col)
            .collect(Collectors.toList());

    // Partitioned columns must be at the end
    if (configuration.isUsePartitionedTable()) {
      columns.add(col("datasetkey"));
    }
    fromAvroToTable(
        spark,
        getSnapshotPath(extensionTable.getDirectoryTableName()), // FROM sourceDir
        columns.toArray(Column[]::new), // SELECT
        extensionTableName(extensionTable)); // INSERT OVERWRITE INTO
  }

  private String extensionTableName(ExtensionTable extensionTable) {
    return String.format(
        "%s_ext_%s", configuration.getTableName(), extensionTable.getHiveTableName());
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
            + " STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"ZSTD\")\n",
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
            + "LOCATION '%s'"
            + " STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"ZSTD\")\n",
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
            + "publisher STRING, license STRING, rightsHolder STRING) \n"
            + "STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"ZSTD\")",
        getPrefix() + multimediaTableName());
  }

  private String multimediaTableName() {
    return String.format("%s_multimedia", configuration.getTableName());
  }

  public void insertOverwriteMultimediaTable(SparkSession spark) {

    spark
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
            callUDF("cleanDelimiters", col("mm_record.rightsHolder")).alias("rightsHolder"))
        .createOrReplaceTempView("mm_records");

    spark.sql(
        String.format(
            "INSERT OVERWRITE TABLE %1$s_multimedia \n"
                + "SELECT gbifid, type, format, identifier, references, title, description, source, audience, created, creator, contributor, publisher, license, rightsHolder FROM mm_records",
            configuration.getTableName()));
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
            + ") STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\")",
        configuration.getTableNameWithPrefix());
  }

  private String createPartitionedTableIfNotExists() {
    return String.format(
        "CREATE EXTERNAL TABLE IF NOT EXISTS %s ("
            + OccurrenceHDFSTableDefinition.definition().stream()
                .filter(
                    field ->
                        configuration.isUsePartitionedTable()
                            && !field
                                .getHiveField()
                                .equalsIgnoreCase("datasetkey")) // Excluding partitioned columns
                .map(field -> field.getHiveField() + " " + field.getHiveDataType())
                .collect(Collectors.joining(", "))
            + ") "
            + "PARTITIONED BY(datasetkey STRING) "
            + "STORED AS PARQUET "
            + "LOCATION '%s'"
            + "TBLPROPERTIES (\"parquet.compression\"=\"ZSTD\", \"auto.purge\"=\"true\")",
        configuration.getTableNameWithPrefix(),
        Paths.get(configuration.getTargetDirectory(), configuration.getCoreName().toLowerCase()));
  }

  private Column[] selectFromAvro() {
    List<Column> columns =
        OccurrenceHDFSTableDefinition.definition().stream()
            .filter(
                field ->
                    !configuration.isUsePartitionedTable()
                        || !field
                            .getColumnName()
                            .equalsIgnoreCase(
                                "datasetkey")) // Partitioned columns must be at the end
            .map(
                field -> {
                  String columnName = field.getColumnName();

                  // Use coalesce to avoid errors if column is missing
                  Column columnExpr = coalesce(col(columnName), lit(null));

                  // Apply transformation if needed
                  return field.getInitializer().equals(columnName)
                      ? columnExpr // No transformation needed
                      : coalesce(
                              callUDF(
                                  field
                                      .getInitializer()
                                      .substring(0, field.getInitializer().indexOf("(")),
                                  columnExpr),
                              lit(null))
                          .alias(columnName);
                })
            .collect(Collectors.toList());

    // Partitioned columns must be at the end
    if (configuration.isUsePartitionedTable()) {
      columns.add(col("datasetkey"));
    }
    Column[] selectColumns = columns.toArray(new Column[] {});
    log.info(
        "Selecting columns from Avro {}",
        columns.stream().map(Column::toString).collect(Collectors.joining(", ")));
    return selectColumns;
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
