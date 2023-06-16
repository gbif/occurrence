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
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructType;

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
    SparkSession.Builder sparkBuilder = SparkSession.builder()
           .appName(configuration.getTableName() + " table build")
           .config("spark.sql.warehouse.dir", configuration.getWarehouseLocation())
           .config("hive.metastore.uris", configuration.getHiveThriftAddress())
           .enableHiveSupport();

    if (configuration.isUsePartitionedTable()) {
      sparkBuilder.config("spark.sql.sources.partitionOverwriteMode","dynamic");
    }
    return sparkBuilder.getOrCreate();
  }

  private void createTableUsingSpark(SparkSession spark) {
    //Create Hive Table if it doesn't exist
    spark.sql(createTableIfNotExists());
    fromAvroToTable(spark,
                    getSnapshotPath(configuration.getCoreName()), //FROM
                    selectFromAvro(), //SELECT
                    configuration.getTableName() //INSERT OVERWRITE INTO
                    );
  }

  private void executeCreateAction(Command command, SparkSession spark) {
    HdfsSnapshotAction snapshotAction = new HdfsSnapshotAction(configuration, spark.sparkContext().hadoopConfiguration());
    try {
      log.info("Using {} as snapshot name of source directory", jobId);
      snapshotAction.createHdfsSnapshot(jobId);
      UDFS.registerUdfs(spark);
      if (command.getOptions().contains(Option.ALL) || command.getOptions().contains(Option.TABLE)) {
        log.info("Creating Avro and Parquet Table for " + configuration.getTableName());
        createTableUsingSpark(spark);
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
      snapshotAction.deleteHdfsSnapshot(jobId);
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
      log.info("Deleting Extension Tables");
      ExtensionTable.tableExtensions().forEach(extensionTable -> {
        String extensionTableName = extensionTableName(extensionTable);
        log.info("Deleting Extension Table {}", extensionTableName);
        spark.sql(dropTable(extensionTableName));
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
    ExtensionTable.tableExtensions().parallelStream()
      .forEach(extensionTable -> createExtensionTable(spark, extensionTable));
  }

  @SneakyThrows
  private static boolean isDirectoryEmpty(String fromSourceDir, SparkSession spark) {
    return FileSystem.get(spark.sparkContext().hadoopConfiguration()).getContentSummary(new Path(fromSourceDir)).getFileCount() == 0;
  }

  private void fromAvroToTable(SparkSession spark, String fromSourceDir, Column[] select, String saveToTable) {
   if(!isDirectoryEmpty(fromSourceDir, spark)) {
     if(configuration.isUsePartitionedTable()) {
       spark.sql(" set hive.exec.dynamic.partition.mode=nonstrict");
     }
     spark.read()
       .format("avro")
       .load(fromSourceDir + "/*.avro")
       .select(select)
       .write()
       .format("parquet")
       .option("compression", "Snappy")
       .mode("overwrite")
       .insertInto(saveToTable);
   }
  }
  private void createExtensionTable(SparkSession spark, ExtensionTable extensionTable) {
    spark.sql(configuration.isUsePartitionedTable()? createExtensionExternalTable(extensionTable) : createExtensionTable(extensionTable));

    List<Column> columns = extensionTable.getFields().stream()
      .filter(field -> configuration.isUsePartitionedTable() && !field.equalsIgnoreCase("datasetkey")) //Excluding partitioned columns
      .map(field -> field.contains(")")? callUDF(field.substring(0, field.indexOf('(')), col(field.substring(field.indexOf('(') + 1, field.lastIndexOf(')')))).alias(field.substring(field.indexOf('(') + 1, field.lastIndexOf(')'))) : col(field))
      .collect(Collectors.toList());

    //Partitioned columns must be at the end
    if (configuration.isUsePartitionedTable()) {
      columns.add(col("datasetkey"));
    }
    fromAvroToTable(spark,
                    getSnapshotPath(extensionTable.getDirectoryTableName()), // FROM sourceDir
                    columns.toArray(new Column[]{}), //SELECT
                    extensionTableName(extensionTable)); //INSERT OVERWRITE INTO
  }

  private String extensionTableName(ExtensionTable extensionTable) {
   return String.format("%s_ext_%s", configuration.getTableName(),extensionTable.getHiveTableName());
  }

  private String createExtensionTable(ExtensionTable extensionTable) {
   return String.format("CREATE TABLE IF NOT EXISTS %s\n"
                        + '(' + extensionTable.getSchema().getFields().stream().map(f -> f.name() + " STRING").collect(
                          Collectors.joining(",\n")) + ')'
                        + "STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"Snappy\")\n",
                        extensionTableName(extensionTable));
  }

  private String createExtensionExternalTable(ExtensionTable extensionTable) {
    return String.format("CREATE EXTERNAL TABLE IF NOT EXISTS %s\n"
                         + '(' + extensionTable.getSchema().getFields().stream().filter(f -> !f.name().equalsIgnoreCase("datasetkey")).map(f -> f.name() + " STRING").collect(
                           Collectors.joining(",\n")) + ')'
                         + "PARTITIONED BY(datasetkey STRING) "
                         + "LOCATION '%s'"
                         + "STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"Snappy\")\n",
                         extensionTableName(extensionTable),
                         Paths.get(configuration.getTargetDirectory(), extensionTable.getHiveTableName()));
  }

  private String dropTable(String tableName) {
    return String.format("DROP TABLE IF EXISTS %s", tableName);
  }

  private String getSnapshotPath(String dataDirectory) {
    String path = Paths.get(configuration.getSourceDirectory(), configuration.getCoreName().toLowerCase(), ".snapshot",
                            jobId, dataDirectory.toLowerCase()).toString();
    log.info("Snapshot path {}", path);
    return path;
  }

  public String createIfNotExistsGbifMultimedia() {
    return String.format("CREATE TABLE IF NOT EXISTS %s_multimedia\n"
                         + "(gbifid STRING, type STRING, format STRING, identifier STRING, references STRING, title STRING, description STRING,\n"
                         + "source STRING, audience STRING, created STRING, creator STRING, contributor STRING,\n"
                         + "publisher STRING, license STRING, rightsHolder STRING)\n"
                         + "STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"Snappy\")", configuration.getTableName());
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


  private String createTableIfNotExists() {
   return configuration.isUsePartitionedTable()? createPartitionedTableIfNotExists(): createParquetTableIfNotExists();
  }

  private String createParquetTableIfNotExists() {
    return String.format("CREATE TABLE IF NOT EXISTS %s (\n"
                         + OccurrenceHDFSTableDefinition.definition().stream()
                           .map(field -> field.getHiveField() + " " + field.getHiveDataType())
                           .collect(Collectors.joining(", \n"))
                         + ") STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"Snappy\")",
                         configuration.getTableName());
  }

  private String createPartitionedTableIfNotExists() {
    return String.format("CREATE EXTERNAL TABLE IF NOT EXISTS %s ("
                         + OccurrenceHDFSTableDefinition.definition().stream()
                           .filter(field -> configuration.isUsePartitionedTable() && !field.getHiveField().equalsIgnoreCase("datasetkey")) //Excluding partitioned columns
                           .map(field -> field.getHiveField() + " " + field.getHiveDataType())
                           .collect(Collectors.joining(", "))
                         + ") "
                         + "PARTITIONED BY(datasetkey STRING) "
                         + "STORED AS PARQUET "
                         + "LOCATION '%s'"
                         + "TBLPROPERTIES (\"parquet.compression\"=\"Snappy\", \"auto.purge\"=\"true\")",
                         configuration.getTableName(),
                         Paths.get(configuration.getTargetDirectory(), configuration.getCoreName().toLowerCase()));

  }

  private Column[] selectFromAvro() {
    List<Column> columns = OccurrenceHDFSTableDefinition.definition().stream()
      .filter(field -> configuration.isUsePartitionedTable() && !field.getHiveField().equalsIgnoreCase("datasetkey")) //Partitioned columns must be at the end
      .map(field -> field.getInitializer().equals(field.getHiveField())?  col(field.getHiveField()) : callUDF(field.getInitializer().substring(0, field.getInitializer().indexOf("(")), col(field.getHiveField())).alias(field.getHiveField()))
      .collect(Collectors.toList());

    //Partitioned columns must be at the end
    if (configuration.isUsePartitionedTable()) {
      columns.add(col("datasetkey"));
    }
    return columns.toArray(new Column[]{});
  }


}
