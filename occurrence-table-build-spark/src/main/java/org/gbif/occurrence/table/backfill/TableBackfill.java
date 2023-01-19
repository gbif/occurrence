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
import org.gbif.occurrence.download.hive.InitializableField;
import org.gbif.occurrence.download.hive.OccurrenceAvroHdfsTableDefinition;
import org.gbif.occurrence.download.hive.OccurrenceHDFSTableDefinition;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructType;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.from_json;

@AllArgsConstructor
@Slf4j
public class TableBackfill {
 private final TableBackfillConfiguration configuration;

 private final String snapshotId = UUID.randomUUID().toString();


 public enum Option {
   TABLE, EXTENSIONS, MULTIMEDIA, ALL;
 }


  public enum Action {
    CREATE, DELETE;
  }

  @Data
  @Builder
  public static class Command {
   private final Action action;
   private final Set<Option> options;

    public static Command parse(String actionArg, String optionsArg) {
      Action action = Action.valueOf(actionArg.toUpperCase());
      Set<Option> options = Arrays.stream(optionsArg.split(",")).map(opt -> Option.valueOf(opt.toUpperCase())).collect(Collectors.toSet());
      return Command.builder().action(action).options(options).build();
    }
  }


  public static void main(String[] args) {
    //Read config file
    TableBackfillConfiguration tableBackfillConfiguration = TableBackfillConfiguration.loadFromFile(args[0]);

    TableBackfill backfill = new TableBackfill(tableBackfillConfiguration);

    backfill.run(Command.parse(args[1], args[2]));
  }

  private SparkSession createSparkSession() {
   return SparkSession.builder()
           .appName(configuration.getTableName() + " table build")
           .config("spark.sql.warehouse.dir", configuration.getWarehouseLocation())
           .enableHiveSupport()
           .getOrCreate();
  }

  private void executeCreateAction(Command command, SparkSession spark) {
    HdfsSnapshotAction snapshotAction = new HdfsSnapshotAction(configuration, spark.sparkContext().hadoopConfiguration());
    try {
      log.info("Using {} as snapshot name of source directory", snapshotId);
      snapshotAction.createHdfsSnapshot(snapshotId);
      registerUdfs().forEach(spark::sql);
      if (command.getOptions().contains(Option.ALL) || command.getOptions().contains(Option.TABLE)) {
        log.info("Creating Avro and Parquet Table for " + configuration.getTableName());
        spark.sql(createTableIfNotExists());
        spark.sql(dropAvroTableIfExists());
        spark.sql(createAvroTempTable());
        spark.sql(insertOverwriteTable());
        spark.sql(dropAvroTableIfExists());
      }

      if (command.getOptions().contains(Option.ALL) || command.getOptions().contains(Option.EXTENSIONS)) {
        log.info("Creating Extension tables");
        createExtensionTablesParallel(spark);
      }

      if (command.getOptions().contains(Option.ALL) || command.getOptions().contains(Option.MULTIMEDIA)) {
        log.info("Creating Multimedia Table");
        spark.sql(createIfNotExistsGbifMultimedia());
        insertOverwriteMultimediaTable(spark);
      }
    } finally {
      snapshotAction.deleteHdfsSnapshot(snapshotId);
      log.info("Creation finished");
    }
  }

  private void executeDeleteAction(Command command, SparkSession spark) {
    if (command.getOptions().contains(Option.ALL) || command.getOptions().contains(Option.TABLE)) {
      log.info("Deleting Table " +  configuration.getTableName());
      spark.sql(dropTable(configuration.getTableName()));
      spark.sql(dropTable(configuration.getTableName() + "_avro"));
    }
    if (command.getOptions().contains(Option.ALL) || command.getOptions().contains(Option.MULTIMEDIA)) {
      log.info("Deleting Multimedia Table ");
      spark.sql(dropTable(configuration.getTableName() + "_multimedia"));
    }
    if (command.getOptions().contains(Option.ALL) || command.getOptions().contains(Option.EXTENSIONS)) {
      log.info("Deleting Extension Tables ");
      ExtensionTable.tableExtensions().forEach(extensionTable -> {
        log.info("Deleting Extension Parquet and Avro Tables for " + extensionTable.getHiveTableName());
        spark.sql(dropTable(String.format("%s_ext_%s", configuration.getTableName(), extensionTable.getHiveTableName())));
        spark.sql(dropTable(String.format("%s_ext_%s_avro", configuration.getTableName(), extensionTable.getHiveTableName())));
      });
    }
  }
  public void run(Command command) {
   try(SparkSession spark = createSparkSession()) {
     spark.sql("USE " + configuration.getHiveDatabase());
     log.info("Running command " + command);
     if (Action.CREATE == command.getAction()) {
       executeCreateAction(command, spark);
     } else if (Action.DELETE == command.getAction()) {
        executeDeleteAction(command, spark);
     }
   }
  }

  private void createExtensionTablesParallel(SparkSession spark) {
    ExtensionTable.tableExtensions().stream().parallel().forEach(extensionTable -> createExtensionTable(spark, extensionTable));
  }

  private void createExtensionTable(SparkSession spark, ExtensionTable extensionTable) {
    spark.sql(deleteAvroExtensionTable(extensionTable));
    spark.sql(createAvroExtensionTable(extensionTable));
    spark.sql(createExtensionTableFromAvro(extensionTable));
    spark.sql(deleteAvroExtensionTable(extensionTable));
  }

  private String dropTable(String tableName) {
    return String.format("DROP TABLE IF EXISTS %s", tableName);
  }
  private String deleteAvroExtensionTable(ExtensionTable extensionTable) {
    return String.format("DROP TABLE IF EXISTS %s_ext_%s_avro", configuration.getTableName(), extensionTable.getHiveTableName());
  }

  private String getSnapshotPath(String dataDirectory) {
    String path = Paths.get(configuration.getSourceDirectory(), configuration.getCoreName().toLowerCase(), ".snapshot", snapshotId, dataDirectory.toLowerCase()).toString();
    log.info("Snapshot path {}", path);
    return path;
  }

  private String createAvroExtensionTable(ExtensionTable extensionTable) {
    return String.format("CREATE EXTERNAL TABLE %s_ext_%s_avro\n"
                         + "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'\n"
                         + "STORED as INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'\n"
                         + "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'\n"
                         + "LOCATION '%s'\n"
                         + "TBLPROPERTIES ('avro.schema.literal'='%s')",
                         configuration.getTableName(),
                         extensionTable.getHiveTableName(),
                         getSnapshotPath(extensionTable.getDirectoryTableName()),
                         extensionTable.getSchema().toString(false));
  }

  private String createExtensionTableFromAvro(ExtensionTable extensionTable) {
    return String.format("CREATE TABLE IF NOT EXISTS %1$s_ext_%2$s\n"
                         + "STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\")\n"
                         + "AS\n"
                         + "SELECT\n"
                         + String.join(",\n", extensionTable.getFields())
                         + "\nFROM %1$s_ext_%2$s_avro", configuration.getTableName(), extensionTable.getHiveTableName());
  }

  public String createIfNotExistsGbifMultimedia() {
    return String.format("CREATE TABLE IF NOT EXISTS %s_multimedia\n"
                         + "(gbifid STRING, type STRING, format STRING, identifier STRING, references STRING, title STRING, description STRING,\n"
                         + "source STRING, audience STRING, created STRING, creator STRING, contributor STRING,\n"
                         + "publisher STRING, license STRING, rightsHolder STRING)\n"
                         + "STORED AS PARQUET", configuration.getTableName());
  }

  public void insertOverwriteMultimediaTable(SparkSession spark) {
    spark.table("occurrence").select(col("gbifid"),
                                     from_json(col("ext_multimedia"),
                                               new ArrayType(new StructType()
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
      .select(col("gbifid"),
              callUDF("cleanDelimiters", col("mm_record.type")).alias("type"),
              callUDF("cleanDelimiters", col("mm_record.format")).alias("format"),
              callUDF("cleanDelimiters",col("mm_record.identifier")).alias("identifier"),
              callUDF("cleanDelimiters", col("mm_record.references")).alias("references"),
              callUDF("cleanDelimiters",col("mm_record.title")).alias("title"),
              callUDF("cleanDelimiters", col("mm_record.description")).alias("description"),
              callUDF("cleanDelimiters",col("mm_record.source")).alias("source"),
              callUDF("cleanDelimiters",col("mm_record.audience")).alias("audience"),
              col("mm_record.created").alias("created"),
              callUDF("cleanDelimiters", col("mm_record.creator")).alias("creator"),
              callUDF("cleanDelimiters",col("mm_record.contributor")).alias("contributor"),
              callUDF("cleanDelimiters",col("mm_record.publisher")).alias("publisher"),
              callUDF("cleanDelimiters",col("mm_record.license")).alias("license"),
              callUDF("cleanDelimiters",col("mm_record.rightsHolder")).alias("rightsHolder"))
      .registerTempTable("mm_records");

    spark.sql(String.format("INSERT OVERWRITE TABLE %1$s_multimedia \n"
                           + "SELECT gbifid, type, format, identifier, references, title, description, source, audience, created, creator, contributor, publisher, license, rightsHolder FROM mm_records",
                            configuration.getTableName()));
  }


  private String dropAvroTableIfExists() {
    return String.format("DROP TABLE IF EXISTS %s_avro", configuration.getTableName());
  }
  private String createTableIfNotExists() {
    return String.format("CREATE TABLE IF NOT EXISTS %s (\n"
                         + OccurrenceHDFSTableDefinition.definition().stream()
                           .map(field -> field.getHiveField() + " " + field.getHiveDataType())
                           .collect(Collectors.joining(", \n"))
                         + ") STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\")",
                         configuration.getTableName());
  }

  private String createAvroTempTable() {
    return String.format("CREATE EXTERNAL TABLE %s_avro\n"
                       + "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'\n"
                       + "STORED as INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'\n"
                       + "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'\n"
                       + "LOCATION '%s'\n"
                       + "TBLPROPERTIES ('avro.schema.literal'='%s')",
                         configuration.getTableName(),
                         getSnapshotPath(configuration.getCoreName()),
                         OccurrenceAvroHdfsTableDefinition.avroDefinition().toString(false));
  }

  private String selectFromAvroQuery() {
    return String.format("SELECT \n" +
           OccurrenceHDFSTableDefinition.definition().stream().map(InitializableField::getInitializer).collect(Collectors.joining(", \n"))
          +
           "\nFROM %s_avro", configuration.getTableName());
  }

  private String insertOverwriteTable() {
    return String.format("INSERT OVERWRITE TABLE %s \n", configuration.getTableName())
           + selectFromAvroQuery();
  }

  public List<String> registerUdfs() {
    return Arrays.asList(
    "CREATE TEMPORARY FUNCTION removeNulls AS 'org.gbif.occurrence.hive.udf.ArrayNullsRemoverGenericUDF'"
    ,"CREATE TEMPORARY FUNCTION cleanDelimiters AS 'org.gbif.occurrence.hive.udf.CleanDelimiterCharsUDF'"
    , "CREATE TEMPORARY FUNCTION cleanDelimitersArray AS 'org.gbif.occurrence.hive.udf.CleanDelimiterArraysUDF'"
    , "CREATE TEMPORARY FUNCTION toISO8601 AS 'org.gbif.occurrence.hive.udf.ToISO8601UDF'"
    , "CREATE TEMPORARY FUNCTION toLocalISO8601 AS 'org.gbif.occurrence.hive.udf.ToLocalISO8601UDF'"
    , "CREATE TEMPORARY FUNCTION stringArrayContains AS 'org.gbif.occurrence.hive.udf.StringArrayContainsGenericUDF'");
  }


}
