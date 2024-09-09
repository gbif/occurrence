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

import org.gbif.occurrence.download.hive.InitializableField;
import org.gbif.occurrence.download.hive.OccurrenceAvroHdfsTableDefinition;
import org.gbif.occurrence.download.hive.OccurrenceHDFSTableDefinition;
import org.gbif.occurrence.spark.udf.UDFS;

import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.spark.sql.SparkSession;

import com.google.common.base.Strings;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

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

  private void createTable(SparkSession spark) {

    ExternalAvroTable avroTable = ExternalAvroTable.create(HdfsSnapshotCoordinator.getSnapshotPath(configuration, configuration.getCoreName(), jobId),
                                                           OccurrenceAvroHdfsTableDefinition.avroDefinition(),
                                                           configuration.getAvroTableName());

    DataTable.from(configuration, spark, "datasetkey", OccurrenceHDFSTableDefinition.definition())
      .createTableIfNotExists()
      .insertOverwriteFromAvro(avroTable, occurrenceTableFields());
  }

  private void executeCreateAction(Command command, SparkSession spark) {
    HdfsSnapshotCoordinator snapshotAction =
        new HdfsSnapshotCoordinator(configuration, spark.sparkContext().hadoopConfiguration());
    try {
      log.info("Using {} as snapshot name of source directory", jobId);
      snapshotAction.createHdfsSnapshot(jobId);
      UDFS.registerUdfs(spark);
      if (command.getOptions().contains(Option.ALL)
          || command.getOptions().contains(Option.TABLE)) {
        log.info("Creating Avro and Parquet Table for " + configuration.getTableName());
        createTable(spark);
      }

      if (command.getOptions().contains(Option.ALL)
          || command.getOptions().contains(Option.EXTENSIONS)) {
        log.info("Creating Extension tables");
        createExtensionTablesParallel(spark);
      }

      if (command.getOptions().contains(Option.ALL)
          || command.getOptions().contains(Option.MULTIMEDIA)) {
        log.info("Creating Multimedia Table");
        createMultimediaTable(spark);
      }
    } finally {
      snapshotAction.deleteHdfsSnapshot(jobId);
      log.info("Creation finished");
    }
  }

  private void executeSchemaMigrationAction(Command command, SparkSession spark) {
    executeDeleteAction(command, spark, "old_");
    if (Strings.isNullOrEmpty(configuration.getPrefixTable())) {
      configuration.setPrefixTable("new");
    }
    executeCreateAction(command, spark);
    swapTables(command, spark);
  }

  private void createMultimediaTable(SparkSession spark) {
    MultimediaTableBackfill multimediaTableBackfill = MultimediaTableBackfill.builder().configuration(configuration).spark(spark).build();
    multimediaTableBackfill.createIfNotExistsGbifMultimedia();
    multimediaTableBackfill.insertOverwriteMultimediaTable();
  }

  private void executeDeleteAction(Command command, SparkSession spark, String prefix) {
    SparkSqlHelper sparkSqlHelper = SparkSqlHelper.of(spark);
    if (command.getOptions().contains(Option.ALL) || command.getOptions().contains(Option.TABLE)) {
      log.info("Deleting Table " + configuration.getTableName());
      sparkSqlHelper.dropTable(prefix + configuration.getTableName());
      sparkSqlHelper.dropTable(prefix + configuration.getTableName() + "_avro");
    }
    if (command.getOptions().contains(Option.ALL)
        || command.getOptions().contains(Option.MULTIMEDIA)) {
      log.info("Deleting Multimedia Table ");
      sparkSqlHelper.dropTable(prefix + configuration.getTableName() + "_multimedia");
    }
    if (command.getOptions().contains(Option.ALL)
        || command.getOptions().contains(Option.EXTENSIONS)) {
      log.info("Deleting Extension Tables");
      ExtensionTablesBulkOperation.of(spark)
        .dropAll(configuration.getCoreName(), prefix);
    }
  }

  public void run(Command command) {
    try (SparkSession spark = createSparkSession()) {
      spark.sql("USE " + configuration.getHiveDatabase());
      log.info("Running command " + command);
      if (Action.CREATE == command.getAction()) {
        executeCreateAction(command, spark);
      } else if (Action.SCHEMA_MIGRATION == command.getAction()) {
        executeSchemaMigrationAction(command, spark);
      } else if (Action.DELETE == command.getAction()) {
        executeDeleteAction(command, spark, "");
      }
    }
  }

  @SneakyThrows
  private void createExtensionTablesParallel(SparkSession spark) {
    ExtensionTablesBulkOperation.of(spark).createExtensionTablesParallel(jobId, configuration);
  }

  private String occurrenceTableFields() {
    return OccurrenceHDFSTableDefinition.definition().stream()
      // Excluding partitioned columns
      .filter(field -> configuration.isUsePartitionedTable() && !field.getHiveField().equalsIgnoreCase("datasetkey"))
      .map(InitializableField::getInitializer)
      .collect(Collectors.joining(", ")) + (configuration.isUsePartitionedTable()? ", datasetkey" : "");
  }

  private void swapTables(Command command, SparkSession spark) {
    if (command.getOptions().contains(Option.ALL) || command.getOptions().contains(Option.TABLE)) {
      log.info("Swapping table: " + configuration.getTableName());
      DataTable.builder()
        .tableName(configuration.getTableName())
        .spark(spark)
        .build()
      .swap("old_", configuration.getTableNameWithPrefix());
    }

    if (command.getOptions().contains(Option.ALL)
        || command.getOptions().contains(Option.EXTENSIONS)) {
      log.info("Swapping Extension tables");
      ExtensionTablesBulkOperation.of(spark).swap(configuration.getCoreName(), "old_", configuration.prefixTableWithUnderscore());
    }

    if (command.getOptions().contains(Option.ALL)
        || command.getOptions().contains(Option.MULTIMEDIA)) {
      log.info("Swapping Multimedia Table");
      MultimediaTableBackfill.builder()
        .configuration(configuration)
        .spark(spark)
        .build()
      .swap("old_");
    }
  }
}
