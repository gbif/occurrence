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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.SparkSession;

import com.google.common.base.Strings;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DataTable {

  private final SparkSession spark;

  private final String tableName;

  private final boolean partitioned;

  private final String partitionColumn;

  private final String partitionValue;

  private final String fields;


  public static DataTable from(TableBackfillConfiguration configuration, SparkSession spark, String partitionColumn, List<InitializableField> fields) {
    return DataTable.builder()
            .spark(spark)
            .partitioned(configuration.isUsePartitionedTable())
            .tableName(configuration.getTableNameWithPrefix())
            .partitionColumn(partitionColumn)
            .partitionValue(configuration.getDatasetKey())
            .fields(fields(partitionColumn, fields))
            .build();
  }

  private static String fields(String partitionColumn, List<InitializableField> fields) {
    if (partitionColumn != null) {
      return fields.stream()
        .filter(field -> !field.getHiveField().equalsIgnoreCase(partitionColumn)) // Excluding partitioned columns
        .map(field -> field.getHiveField() + " " + field.getHiveDataType())
        .collect(Collectors.joining(", "));
    } else {
      return fields.stream()
        .map(field -> field.getHiveField() + " " + field.getHiveDataType())
        .collect(Collectors.joining(", \n"));
    }
  }

  public DataTable createTableIfNotExists() {
    if (partitioned) {
      createPartitionedTableIfNotExists();
    } else {
      createParquetTableIfNotExists();
    }
    return this;
  }

  private void createParquetTableIfNotExists() {
    spark.sql("CREATE TABLE IF NOT EXISTS " + tableName +
              " (" + fields +") STORED AS PARQUET TBLPROPERTIES ('parquet.compression'='SNAPPY')");
  }

  private void createPartitionedTableIfNotExists() {
    spark.sql( "CREATE TABLE IF NOT EXISTS " + tableName +
            " (" + fields + ") USING iceberg PARTITIONED BY(" + partitionColumn + " STRING) " +
            "TBLPROPERTIES ('parquet.compression'='GZIP', 'auto.purge'='true')");
  }

  public void insertOverwriteFromAvro(ExternalAvroTable sourceAvroTable, String selectFields) {
    if (sourceAvroTable.isSourceLocationNotEmpty(spark.sparkContext().hadoopConfiguration())) {
      if (partitioned) {
        spark.sql(" set hive.exec.dynamic.partition.mode=nonstrict");
      }
      sourceAvroTable.reCreate(spark);
      insertOverwrite(tableName, selectFields, sourceAvroTable.getTableName(), partitionColumn, partitionValue);
    }
  }

  private void insertOverwrite(String targetTableName, String selectFields, String sourceTable, String partitionColumn, String partitionValue) {
    String partitionClause = (!Strings.isNullOrEmpty(partitionValue)? " PARTITION (" + partitionColumn + " = '" + partitionValue + "') " : " ");
    spark.sql("INSERT OVERWRITE TABLE " + targetTableName +
              partitionClause +
             "SELECT " + selectFields + " FROM " + sourceTable);
  }

  public void swap(String oldPrefix, String newPrefix) {
    SparkSqlHelper sparkSqlHelper = SparkSqlHelper.of(spark);
    sparkSqlHelper.renameTable(tableName, oldPrefix  + sparkSqlHelper);
    sparkSqlHelper.renameTable(newPrefix, tableName);
  }

}
