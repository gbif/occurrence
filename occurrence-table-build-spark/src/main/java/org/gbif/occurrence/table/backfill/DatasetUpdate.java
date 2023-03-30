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

import org.gbif.occurrence.download.hive.OccurrenceHDFSTableDefinition;
import org.gbif.occurrence.spark.udf.UDFS;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;

import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

/**
 * Utility class to update a Hive tables partitioned by dataset keys.
 */
@Data
@Builder
public class DatasetUpdate {

  /**
   * Supported commands: INSERT/OVERWRITE or DELETE partitions.
   */
  public enum Command {
      UPSERT, DELETE;
  }

  public static void main(String[] args) {
    DatasetUpdate.DatasetUpdateBuilder builder = DatasetUpdate.builder()
      .datasetKey(UUID.fromString(args[1]))
      .database(args[2])
      .tableName(args[3])
      .warehouseLocation(args[4]);

    //Source dir is ignored to delete partitions
    if (args.length > 5) {
      builder.sourceDir(args[5]);
    }

    builder
      .build()
      .run(Command.valueOf(args[0].toUpperCase()));
  }

  //Partition name
  private final UUID datasetKey;

  //Hive database name
  private final String database;

  //Hive table name
  private final String tableName;

  //Hive warehouse location
  private final String warehouseLocation;

  //Used when updating partitions
  @Builder.Default
  private String sourceDir = null;


  private SparkSession createSparkSession() {
    return SparkSession.builder()
            .appName("Dataset " + datasetKey + " tables update")
            .config("spark.sql.warehouse.dir", warehouseLocation)
             //dynamic partition overwriting
            .config("spark.sql.sources.partitionOverwriteMode","dynamic")
            .enableHiveSupport()
            .getOrCreate();
  }

  @SneakyThrows
  private static boolean isDirectoryEmpty(String fromSourceDir, SparkSession spark) {
    return FileSystem.get(spark.sparkContext().hadoopConfiguration()).getContentSummary(new Path(fromSourceDir)).getFileCount() == 0;
  }

  /**
   * Executes the input command.
   */
  public void run(Command command) {
    try(SparkSession spark = createSparkSession()) {
      spark.sql("USE " + database);
      if (Command.DELETE == command) {
        dropPartition(spark);
      } else if (Command.UPSERT == command) {
        createOrUpdatePartition(spark);
      }
    }
  }

  /**
   * Drops the dataset partition.
   */
  private void dropPartition(SparkSession spark) {
    spark.sql("ALTER TABLE " + tableName + " DROP IF EXISTS PARTITION (datasetkey='" + datasetKey + "')");
  }

  /**
   * Performs an INSERT OVERWRITE into the target table.
   */
  private void createOrUpdatePartition(SparkSession spark) {
    if(!isDirectoryEmpty(sourceDir, spark)) {
      spark.sql(" set hive.exec.dynamic.partition.mode=nonstrict");
      UDFS.registerUdfs(spark);
      spark.read()
        .format("com.databricks.spark.avro")
        .load(sourceDir + "/*.avro")
        .select(selectFromAvro())
        .write()
        .format("parquet")
        .option("compression", "snappy")
        .mode("overwrite")
        .insertInto(tableName);
    }
  }

  /**
   * Table select statement.
   */
  private Column[] selectFromAvro() {
    List<Column> columns = OccurrenceHDFSTableDefinition.definition().stream()
      .filter(field -> !field.getHiveField().equalsIgnoreCase("datasetkey")) //Partitioned columns must be at the end
      .map(field -> field.getInitializer().equals(field.getHiveField())?  col(field.getHiveField()) : callUDF(field.getInitializer().substring(0, field.getInitializer().indexOf("(")), col(field.getHiveField())).alias(field.getHiveField()))
      .collect(Collectors.toList());

    //Partitioned columns must be at the end
    columns.add(col("datasetkey"));

    return columns.toArray(new Column[]{});
  }


}
