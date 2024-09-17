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

import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.spark.sql.SparkSession;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ExtensionTableBackfill {

  private final TableBackfillConfiguration configuration;

  private final ExtensionTable extensionTable;

  private final SparkSession spark;

  private final String jobId;


  public void createTable() {
    spark.sql(
      configuration.isUsePartitionedTable()
        ? createExtensionPartitionedTable(extensionTable)
        : createExtensionTable(extensionTable));

    String select =
      extensionTable.getFields().stream()
        .filter(
          field ->
            !configuration.isUsePartitionedTable()
              || !field.equalsIgnoreCase("datasetkey")) // Excluding partitioned columns
        .collect(Collectors.joining(",")) + ", datasetkey";

    ExternalAvroTable avroTable =
    ExternalAvroTable.create(HdfsSnapshotCoordinator.getSnapshotPath(configuration, extensionTable.getDirectoryTableName(), jobId), extensionTable.getSchema(), extensionAvroTableName(extensionTable));

    datatable(configuration, spark, "datasetkey", extensionTable.getSchema(), extensionTableName(extensionTable))
      .createTableIfNotExists()
      .insertOverwriteFromAvro(avroTable, select);
  }

  public static DataTable datatable(TableBackfillConfiguration configuration, SparkSession spark, String partitionColumn, Schema schema, String tableName) {
    return DataTable.builder()
      .spark(spark)
      .partitioned(configuration.isUsePartitionedTable())
      .tableName(tableName)
      .partitionColumn(partitionColumn)
      .partitionValue(configuration.getDatasetKey())
      .fields(schema.getFields()
        .stream()
        .filter(field -> !field.name().equalsIgnoreCase(partitionColumn))
        .map(field -> field.name() + " STRING")
        .collect(Collectors.joining(",\n")))
      .build();
  }

  private String extensionTableName(ExtensionTable extensionTable) {
    return extensionTableName(configuration.getTableName(), extensionTable);
  }

  public static String extensionTableName(String coreTableName, ExtensionTable extensionTable) {
    return coreTableName + "_ext_" + extensionTable.getHiveTableName();
  }

  private String extensionAvroTableName(ExtensionTable extensionTable) {
    return extensionAvroTableName(configuration.getTableName(), extensionTable);
  }

  public static String extensionAvroTableName(String coreTableName, ExtensionTable extensionTable) {
    return coreTableName+ "_ext" + extensionTable.getHiveTableName() + "_avro";
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
      configuration.prefixTableWithUnderscore() + extensionTableName(extensionTable));
  }

  private String createExtensionPartitionedTable(ExtensionTable extensionTable) {
    return String.format(
      "CREATE TABLE IF NOT EXISTS %s\n"
        + '('
        + extensionTable.getSchema().getFields().stream()
        .filter(f -> !f.name().equalsIgnoreCase("datasetkey"))
        .map(f -> f.name() + " STRING")
        .collect(Collectors.joining(",\n"))
        + ')'
        + " USING iceberg PARTITIONED BY(datasetkey STRING) \n"
        + "TBLPROPERTIES ('iceberg.catalog'='location_based_table')\n",
      configuration.prefixTableWithUnderscore() + extensionTableName(extensionTable));
  }
}
