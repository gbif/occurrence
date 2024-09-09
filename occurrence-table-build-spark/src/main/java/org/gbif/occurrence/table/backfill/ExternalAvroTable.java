package org.gbif.occurrence.table.backfill;

import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

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
