<#--
  This is a freemarker template which will generate an HQL script which is run at download time.
  When run in Hive as a parameterized query, this will create a set of tables ...
-->
<#-- Required syntax to escape Hive parameters. Outputs "USE ${hiveDB};" -->
USE ${r"${hiveDB}"};

SET hive.merge.mapfiles=false;
-- Increases memory to avoid a "Container … is running beyond physical memory limits." error.
SET yarn.app.mapreduce.am.resource.mb=32000;

CREATE TEMPORARY FUNCTION toISO8601 AS 'org.gbif.occurrence.hive.udf.ToISO8601UDF';
CREATE TEMPORARY FUNCTION toLocalISO8601 AS 'org.gbif.occurrence.hive.udf.ToLocalISO8601UDF';
CREATE TEMPORARY FUNCTION contains AS 'org.gbif.occurrence.hive.udf.ContainsUDF';
CREATE TEMPORARY FUNCTION geoDistance AS 'org.gbif.occurrence.hive.udf.GeoDistanceUDF';

-- in case this job is relaunched
DROP TABLE IF EXISTS ${r"${occurrenceTable}"};
DROP TABLE IF EXISTS ${r"${occurrenceTable}"}_citation;

-- pre-create verbatim table so it can be used in the multi-insert
CREATE TABLE ${r"${occurrenceTable}"}
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY")
AS SELECT
<#list hiveFields as key, field>
  ${field.hiveField} AS ${parquetFields[key].hiveField}<#if key_has_next>,</#if>
</#list>
FROM occurrence
WHERE ${r"${whereClause}"};

-- creates the citations table, citation table is not compressed since it is read later from Java as TSV.
SET mapred.output.compress=false;
SET hive.exec.compress.output=false;
SET mapred.reduce.tasks=1;

-- See https://github.com/gbif/occurrence/issues/28#issuecomment-432958372
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

CREATE TABLE ${r"${occurrenceTable}"}_citation ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT datasetkey, count(*) as num_occurrences, license
FROM ${r"${occurrenceTable}"}
WHERE datasetkey IS NOT NULL
GROUP BY datasetkey, license;
