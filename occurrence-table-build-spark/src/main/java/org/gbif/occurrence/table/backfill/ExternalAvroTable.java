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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

@Data
@Builder
public class ExternalAvroTable {

  private final String tableName;
  private final Schema schema;
  private final String location;

  public static ExternalAvroTable create(String location, Schema schema, String tableName) {
    return ExternalAvroTable.builder()
      .tableName(tableName)
      .location(location)
      .schema(schema)
      .build();
  }

  public void drop(SparkSession spark) {
    spark.sql("DROP TABLE IF EXISTS " + tableName);
  }

  public void create(SparkSession spark) {
    spark.sql("CREATE EXTERNAL TABLE " + tableName + " USING AVRO " +
                      "OPTIONS( 'format' = 'avro', 'schema' = '" + schema.toString(true) + "') " +
                      "LOCATION '" + location +  "' TBLPROPERTIES('iceberg.catalog'='location_based_table')");
  }

  public void reCreate(SparkSession spark) {
    // Recreate the table
    drop(spark);
    create(spark);
  }

  @SneakyThrows
  public boolean isSourceLocationEmpty(Configuration hadoopConfiguration) {
    return FileSystem.get(hadoopConfiguration)
      .getContentSummary(new Path(location))
      .getFileCount() != 0;
  }
}
