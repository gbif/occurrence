<#-- @ftlvariable name="field" type="org.gbif.occurrence.download.hive.Field" -->
<#--
  This is a freemarker template which will generate an HQL script.
  When run in Hive as a parameterized query, this will create a Hive table from Avro files.
-->

<#-- Required syntax to escape Hive parameters. Outputs "USE ${hive_db};" -->
USE ${r"${hiveDB}"};

-- Creates the temporary Avro table pointing to the snapshot
DROP TABLE IF EXISTS ${r"${tableName}"}_avro;
CREATE EXTERNAL TABLE ${r"${tableName}"}_avro
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED as INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${r"${sourceDataDir}"}/.snapshot/${r"${snapshot}"}/${r"${tableSourceDir}"}'
TBLPROPERTIES ('avro.schema.url'='${r"${wfPath}"}/avro-schemas/occurrence-hdfs-record.avsc');

-- snappy compression
SET hive.exec.compress.output=true;
SET mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

-- hint: ensure these are on the job classpath
CREATE TEMPORARY FUNCTION removeNulls AS 'org.gbif.occurrence.hive.udf.ArrayNullsRemoverGenericUDF';
CREATE TEMPORARY FUNCTION cleanDelimiters AS 'org.gbif.occurrence.hive.udf.CleanDelimiterCharsUDF';
CREATE TEMPORARY FUNCTION cleanDelimitersArray AS 'org.gbif.occurrence.hive.udf.CleanDelimiterArraysUDF';
CREATE TEMPORARY FUNCTION toISO8601 AS 'org.gbif.occurrence.hive.udf.ToISO8601UDF';
CREATE TEMPORARY FUNCTION toLocalISO8601 AS 'org.gbif.occurrence.hive.udf.ToLocalISO8601UDF';
CREATE TEMPORARY FUNCTION from_json AS 'brickhouse.udf.json.FromJsonUDF';
CREATE TEMPORARY FUNCTION stringArrayContains AS 'org.gbif.occurrence.hive.udf.StringArrayContainsGenericUDF';

-- re-create the HDFS table
CREATE TABLE IF NOT EXISTS ${r"${tableName}"} (
<#list fields as field>
  ${field.hiveField} ${field.hiveDataType}<#if field_has_next>,</#if>
</#list>
) STORED AS ORC TBLPROPERTIES ("serialization.null.format"="","orc.compress.size"="65536","orc.compress"="ZLIB");

-- populate the HDFS view
INSERT OVERWRITE TABLE ${r"${tableName}"}
SELECT
<#list fields as field>
  ${field.initializer}<#if field_has_next>,</#if>
</#list>
FROM ${r"${tableName}"}_avro;

<#list extensions as extension>
-- ${extension.extension} Avro external table
DROP TABLE IF EXISTS ${r"${tableName}"}_ext_${extension.hiveTableName}_avro;
CREATE EXTERNAL TABLE ${r"${tableName}"}_ext_${extension.hiveTableName}_avro
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED as INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${r"${sourceDataDir}"}/.snapshot/${r"${snapshot}"}/${extension.directoryTableName}'
TBLPROPERTIES ('avro.schema.url'='${r"${wfPath}"}/avro-schemas/${extension.avroSchemaFileName}');

-- ${extension.extension} extension
CREATE TABLE IF NOT EXISTS ${r"${tableName}"}_ext_${extension.hiveTableName}
STORED AS ORC TBLPROPERTIES ("serialization.null.format"="","orc.compress.size"="65536","orc.compress"="ZLIB")
AS
SELECT
<#list extension.fields as field>
  ${field}<#if field_has_next>,</#if>
</#list>
FROM ${r"${tableName}"}_ext_${extension.hiveTableName}_avro;
</#list>

SET hive.vectorized.execution.reduce.enabled=false;
--this flag is turn OFF to avoid memory exhaustion errors http://hortonworks.com/community/forums/topic/mapjoinmemoryexhaustionexception-on-local-job/
SET hive.auto.convert.join=false;
--https://stackoverflow.com/questions/29946841/hive-kryo-exception
SET hive.exec.parallel=false;

CREATE TABLE IF NOT EXISTS ${r"${tableName}"}_multimedia
(gbifid STRING, type STRING, format STRING, identifier STRING, references STRING, title STRING, description STRING,
source STRING, audience STRING, created STRING, creator STRING, contributor STRING,
publisher STRING,license STRING, rightsHolder STRING)
STORED AS PARQUET;

INSERT OVERWRITE TABLE ${r"${tableName}"}_multimedia
SELECT gbifid, cleanDelimiters(mm_record['type']), cleanDelimiters(mm_record['format']), cleanDelimiters(mm_record['identifier']), cleanDelimiters(mm_record['references']), cleanDelimiters(mm_record['title']), cleanDelimiters(mm_record['description']), cleanDelimiters(mm_record['source']), cleanDelimiters(mm_record['audience']), mm_record['created'], cleanDelimiters(mm_record['creator']), cleanDelimiters(mm_record['contributor']), cleanDelimiters(mm_record['publisher']), cleanDelimiters(mm_record['license']), cleanDelimiters(mm_record['rightsHolder'])
FROM (SELECT occ.gbifid, occ.ext_multimedia  FROM ${r"${tableName}"} occ)
occ_mm LATERAL VIEW explode(from_json(occ_mm.ext_multimedia, 'array<map<string,string>>')) x AS mm_record;

SET hive.auto.convert.join=true;
SET hive.vectorized.execution.reduce.enabled=true;
SET hive.exec.parallel=true;

-- Drop the temporary Avro table
DROP TABLE IF EXISTS ${r"${tableName}"}_avro;
