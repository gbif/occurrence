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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructType;

import com.google.common.base.Strings;

import lombok.Builder;
import lombok.Data;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

@Data
@Builder
public class MultimediaTableBackfill {

  private TableBackfillConfiguration configuration;

  private SparkSession spark;

  private String multimediaTableName() {
    return configuration.getTableNameWithPrefix() + "_multimedia";
  }

  public void createIfNotExistsGbifMultimedia() {
    // Construct the table name with the optional prefix
    String tableName = configuration.prefixTableWithUnderscore() + multimediaTableName();

    // Define the SQL query for creating the table if it doesn't exist
    String createTableQuery = String.format(
      "CREATE TABLE IF NOT EXISTS %s (\n" +
        "    gbifid STRING,\n" +
        "    type STRING,\n" +
        "    format STRING,\n" +
        "    identifier STRING,\n" +
        "    references STRING,\n" +
        "    title STRING,\n" +
        "    description STRING,\n" +
        "    source STRING,\n" +
        "    audience STRING,\n" +
        "    created STRING,\n" +
        "    creator STRING,\n" +
        "    contributor STRING,\n" +
        "    publisher STRING,\n" +
        "    license STRING,\n" +
        "    rightsHolder STRING\n" +
        ") USING iceberg \n" +
        // Add partitioning if configured
        (configuration.isUsePartitionedTable() ? "PARTITIONED BY (datasetkey STRING) " : "") +
        "TBLPROPERTIES ('parquet.compression' = 'GZIP')",
      tableName
    );

    // Execute the SQL query using Spark
    spark.sql(createTableQuery);
  }

  public void insertOverwriteMultimediaTable() {
    createMultimediaRecordsView();

    spark.sql(insertOverWriteMultimediaTable());
  }

  private void createMultimediaRecordsView() {
    Dataset<Row> mmRecords = spark
      .table(configuration.getTableNameWithPrefix())
      .select(
        col("gbifid"),
        from_json(
          col("ext_multimedia"),
          new ArrayType(
            new StructType()
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
          .alias("mm_record"),
        col("datasetkey"))
      .select(col("gbifid"), explode(col("mm_record")).alias("mm_record"), col("datasetkey"))
      .select(
        col("gbifid"),
        callUDF("cleanDelimiters", col("mm_record.type")).alias("type"),
        callUDF("cleanDelimiters", col("mm_record.format")).alias("format"),
        callUDF("cleanDelimiters", col("mm_record.identifier")).alias("identifier"),
        callUDF("cleanDelimiters", col("mm_record.references")).alias("references"),
        callUDF("cleanDelimiters", col("mm_record.title")).alias("title"),
        callUDF("cleanDelimiters", col("mm_record.description")).alias("description"),
        callUDF("cleanDelimiters", col("mm_record.source")).alias("source"),
        callUDF("cleanDelimiters", col("mm_record.audience")).alias("audience"),
        col("mm_record.created").alias("created"),
        callUDF("cleanDelimiters", col("mm_record.creator")).alias("creator"),
        callUDF("cleanDelimiters", col("mm_record.contributor")).alias("contributor"),
        callUDF("cleanDelimiters", col("mm_record.publisher")).alias("publisher"),
        callUDF("cleanDelimiters", col("mm_record.license")).alias("license"),
        callUDF("cleanDelimiters", col("mm_record.rightsHolder")).alias("rightsHolder"),
        col("datasetkey"));
    if (configuration.getDatasetKey() != null) {
      mmRecords = mmRecords.where("datasetkey = '" + configuration.getDatasetKey() + "'");
    }
    mmRecords.createOrReplaceTempView("mm_records");
  }

  private String insertOverWriteMultimediaTable() {
    String partitionClause = Strings.isNullOrEmpty(configuration.getDatasetKey())
      ? ""
      : " PARTITION (datasetkey = '" + configuration.getDatasetKey() + "') ";

    String selectClause = "SELECT gbifid, type, format, identifier, references, title, description, " +
      "source, audience, created, creator, contributor, publisher, license, " +
      "rightsHolder" + (!configuration.isUsePartitionedTable()? ", datasetkey": "") +  " FROM mm_records";

    return "INSERT OVERWRITE TABLE " +  multimediaTableName() + "\n" + partitionClause + selectClause;
  }

  public void swap(String oldPrefix) {
    SparkSqlHelper sparkSqlHelper = SparkSqlHelper.of(spark);
    sparkSqlHelper.renameTable(multimediaTableName(), oldPrefix + multimediaTableName());
    sparkSqlHelper.renameTable(configuration.prefixTableWithUnderscore() + multimediaTableName(), multimediaTableName());
  }
}
