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

import lombok.extern.slf4j.Slf4j;
import org.gbif.occurrence.download.hive.InitializableField;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.sql.SparkSession;

import com.google.common.base.Strings;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
@Slf4j
public class DataTable {

  private final SparkSession spark;

  private final String tableName;

  private final boolean partitioned;

  private final String partitionColumn;

  private final String partitionValue;

  private final Map<String,String> fields;


  public static DataTable from(TableBackfillConfiguration configuration, SparkSession spark, String partitionColumn, List<InitializableField> fields) {
    return DataTable.builder()
            .spark(spark)
            .partitioned(configuration.isUsePartitionedTable())
            .tableName(configuration.getTableNameWithPrefix())
            .partitionColumn(partitionColumn)
            .partitionValue(configuration.getDatasetKey())
            .fields(fields.stream().collect(Collectors.toMap(InitializableField::getHiveField, InitializableField::getHiveDataType)))
            .build();
  }

  private String createTableFields() {
    return fields.entrySet().stream()
      .filter(field -> !partitioned || !field.getKey().equalsIgnoreCase(partitionColumn))
      .map(field -> field.getKey() + " " + field.getValue())
      .collect(Collectors.joining(", \n"));
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
              " (" + createTableFields() +") STORED AS PARQUET TBLPROPERTIES ('parquet.compression'='SNAPPY')");
  }

  private void createPartitionedTableIfNotExists() {
    spark.sql( "CREATE TABLE IF NOT EXISTS " + tableName +
            " (" + createTableFields() + ") USING iceberg PARTITIONED BY(" + partitionColumn + " STRING) " +
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
    String sqlClause = "INSERT OVERWRITE TABLE " + targetTableName + partitionClause + "SELECT " + selectFields + " FROM " + sourceTable;
    log.info("Executing SQL clause {}", sqlClause);
    spark.sql(sqlClause);
  }

  public void swap(String oldPrefix, String newPrefix) {
    SparkSqlHelper sparkSqlHelper = SparkSqlHelper.of(spark);
    sparkSqlHelper.renameTable(tableName, oldPrefix  + sparkSqlHelper);
    sparkSqlHelper.renameTable(newPrefix, tableName);
  }

}
