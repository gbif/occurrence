package org.gbif.occurrence.table.backfill;

import org.gbif.occurrence.download.hive.InitializableField;
import org.gbif.occurrence.download.hive.OccurrenceAvroHdfsTableDefinition;
import org.gbif.occurrence.download.hive.OccurrenceHDFSTableDefinition;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.SparkSession;

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
    spark.sql(createTableIfNotExists(configuration.getTableName()));
    spark.sql(createAvroTempTable());
    spark.sql(insertOverwriteTable());

  }


  private String createTableIfNotExists(String tableName) {
    return String.format("CREATE TABLE IF NOT EXISTS %s (\n"
                         + OccurrenceHDFSTableDefinition.definition().stream()
                           .map(field -> field.getHiveField() + " " + field.getHiveDataType())
                           .collect(Collectors.joining(", \n"))
                         + ") STORED AS PARQUET TBLPROPERTIES (\"parquet.compression\"=\"SNAPPY\")",
                         tableName);
  }

  private String addTableDefaultPartition(String tableName) {
    return String.format("ALTER TABLE %s ADD PARTITION (executionid=1)", tableName);
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
                         OccurrenceAvroHdfsTableDefinition.avroDefinition().toString(true));
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
    //+ "CREATE TEMPORARY FUNCTION from_json AS 'brickhouse.udf.json.FromJsonUDF';\n"
    , "CREATE TEMPORARY FUNCTION stringArrayContains AS 'org.gbif.occurrence.hive.udf.StringArrayContainsGenericUDF'");
  }


}
