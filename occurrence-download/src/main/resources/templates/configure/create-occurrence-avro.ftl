<#-- @ftlvariable name="field" type="org.gbif.occurrence.download.hive.HBaseField" -->
<#--
  This is a freemarker template which will generate an HQL script.
  When run in Hive as a parameterized query, this will create a Hive table which is a
  backed by the HBase table.
-->

<#-- Required syntax to escape Hive parameters. Outputs "USE ${hive_db};" -->
USE ${r"${hiveDB}"};

DROP TABLE IF EXISTS occurrence_pipelines_avro;
CREATE EXTERNAL TABLE occurrence_pipelines_avro
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED as INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/data/ingest/hdfsview/'
TBLPROPERTIES ('avro.schema.url'='/occurrence-download-workflows-${r"${env}"}/create-tables/avro-schemas/occurrence-hdfs-record.avsc');

-- snappy compression
SET hive.exec.compress.output=true;
SET mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

-- configure for reading HBase
SET hbase.client.scanner.caching=400;
SET hive.mapred.reduce.tasks.speculative.execution=false;
SET hive.hadoop.supports.splittable.combineinputformat=true;
SET mapred.max.split.size=256000000;

-- hint: ensure these are on the job classpath
CREATE TEMPORARY FUNCTION collectMediaTypes AS 'org.gbif.occurrence.hive.udf.CollectMediaTypesUDF';
CREATE TEMPORARY FUNCTION removeNulls AS 'org.gbif.occurrence.hive.udf.ArrayNullsRemoverGenericUDF';
CREATE TEMPORARY FUNCTION cleanDelimiters AS 'org.gbif.occurrence.hive.udf.CleanDelimiterCharsUDF';
CREATE TEMPORARY FUNCTION toISO8601 AS 'org.gbif.occurrence.hive.udf.ToISO8601UDF';
CREATE TEMPORARY FUNCTION from_json AS 'brickhouse.udf.json.FromJsonUDF';

-- re-create the HDFS view of the HBase table
CREATE TABLE IF NOT EXISTS occurrence_hdfs (
<#list fields as field>
  ${field.hiveField} ${field.hiveDataType}<#if field_has_next>,</#if>
</#list>
) STORED AS ORC TBLPROPERTIES ("serialization.null.format"="","orc.compress.size"="65536","orc.compress"="ZLIB");

-- populate the HDFS view
INSERT OVERWRITE TABLE occurrence_hdfs
SELECT
<#list fields as field>
  ${field.initializer}<#if field_has_next>,</#if>
</#list>
FROM occurrence_pipelines_avro;
