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

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class TableBackfill {
 private final TableBackfillConfiguration configuration;

  public static void main(String[] args) {
    //Read config file
    TableBackfillConfiguration tableBackfillConfiguration = TableBackfillConfiguration.loadFromFile(args[0]);

    //Execution unique identifier
    String wfId = UUID.randomUUID().toString();

    HdfsSnapshotAction snapshotAction = new HdfsSnapshotAction(tableBackfillConfiguration);
    //snapshotAction.createHdfsSnapshot(wfId);

    TableBackfill backfill = new TableBackfill(tableBackfillConfiguration);
    backfill.createTable();

    //snapshotAction.deleteHdfsSnapshot(wfId);
  }

  public void createTable() {

    SparkSession spark = SparkSession.builder()
                          .appName(configuration.getTableName() + " table build")
                          .config("spark.sql.warehouse.dir", configuration.getWarehouseLocation())
                          .enableHiveSupport()
                          .getOrCreate();
    spark.sql("USE " + configuration.getHiveDatabase());
    registerUdfs().forEach(spark::sql);
    /*spark.sql(createTableIfNotExists());
    spark.sql(dropAvroTableIfExists());
    spark.sql(createAvroTempTable());
    spark.sql(insertOverwriteTable());*/

    //GBIF Create Extension Tables
    createExtensionTables(spark);

    //GBIF Multimedia table
    spark.sql(createIfNotExistsGbifMultimedia());
    spark.sql(insertOverwriteMultimediaTable());

  }

  public void createExtensionTables(SparkSession spark) {
    ExtensionTable.tableExtensions().forEach(extensionTable ->  createExtensionTable(spark, extensionTable));
  }

  public void createExtensionTablesParallel(SparkSession spark) {
   List<CompletableFuture<Dataset<Row>>> futures = ExtensionTable.tableExtensions().stream()
      .map(extensionTable -> CompletableFuture.supplyAsync(() -> createExtensionTable(spark, extensionTable))).collect(Collectors.toList());
    wait(futures);
  }


  public static CompletableFuture<List<Dataset<Row>>> wait(List<CompletableFuture<Dataset<Row>>> futures) {
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
      .thenApply(ignored -> futures.stream()
        .map(CompletableFuture::join)
        .collect(Collectors.toList())
      );
  }

  private Dataset<Row> createExtensionTable(SparkSession spark, ExtensionTable extensionTable) {
    spark.sql(deleteAvroExtensionTable(extensionTable));
    spark.sql(createAvroExtensionTable(extensionTable));
    return spark.sql(createExtensionTableFromAvro(extensionTable));
  }

  private String deleteAvroExtensionTable(ExtensionTable extensionTable) {
    return String.format("DROP TABLE IF EXISTS %s_ext_%s_avro", configuration.getTableName(), extensionTable.getHiveTableName());
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
                         configuration.getSourceDirectory() + '/' + extensionTable.getDirectoryTableName(),
                         extensionTable.getSchema().toString(false));
  }

  private String createExtensionTableFromAvro(ExtensionTable extensionTable) {
    return String.format("CREATE TABLE IF NOT EXISTS %1$s_ext%2$s\n"
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
                         + "publisher STRING,license STRING, rightsHolder STRING)\n"
                         + "STORED AS PARQUET", configuration.getTableName());
  }

  public String insertOverwriteMultimediaTable() {
    return String.format("INSERT OVERWRITE TABLE %1$s_multimedia\n"
                         + "SELECT gbifid, cleanDelimiters(mm_record.type), cleanDelimiters(mm_record.format), cleanDelimiters(mm_record.identifier), cleanDelimiters(mm_record.references), cleanDelimiters(mm_record.title), cleanDelimiters(mm_record.description), cleanDelimiters(mm_record.source), cleanDelimiters(mm_record.audience), mm_record.created, cleanDelimiters(mm_record.creator), cleanDelimiters(mm_record.contributor), cleanDelimiters(mm_record.publisher), cleanDelimiters(mm_record.license), cleanDelimiters(mm_record.rightsHolder)\n"
                         + "FROM (SELECT occ.gbifid, occ.ext_multimedia  FROM %1$s occ)\n"
                         + "occ_mm LATERAL VIEW explode(json_tuple(occ_mm.ext_multimedia)) x AS mm_record", configuration.getTableName());
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
                         configuration.getSourceDirectory(),
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
