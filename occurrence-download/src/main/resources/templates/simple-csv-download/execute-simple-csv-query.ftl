<#--
  This is a freemarker template which will generate an HQL script which is run at download time.
  When run in Hive as a parameterized query, this will create a set of tables ...
-->
<#-- Required syntax to escape Hive parameters. Outputs "USE ${hiveDB};" -->
USE ${r"${hiveDB}"};

-- setup for our custom, combinable deflated compression
-- See https://github.com/gbif/occurrence/issues/28#issuecomment-432958372
SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK;
SET mapred.output.compression.codec=org.gbif.hadoop.compress.d2.D2Codec;
SET io.compression.codecs=org.gbif.hadoop.compress.d2.D2Codec;
SET hive.merge.mapfiles=false;
SET hive.merge.mapredfiles=false;

CREATE TEMPORARY FUNCTION contains AS 'org.gbif.occurrence.hive.udf.ContainsUDF';
CREATE TEMPORARY FUNCTION geoDistance AS 'org.gbif.occurrence.hive.udf.GeoDistanceUDF';
CREATE TEMPORARY FUNCTION toISO8601 AS 'org.gbif.occurrence.hive.udf.ToISO8601UDF';
CREATE TEMPORARY FUNCTION toLocalISO8601 AS 'org.gbif.occurrence.hive.udf.ToLocalISO8601UDF';
CREATE TEMPORARY FUNCTION joinArray AS 'brickhouse.udf.collect.JoinArrayUDF';
CREATE TEMPORARY FUNCTION stringArrayContains AS 'org.gbif.occurrence.hive.udf.StringArrayContainsGenericUDF';

-- in case this job is relaunched
DROP TABLE IF EXISTS ${r"${downloadTableName}"};
DROP TABLE IF EXISTS ${r"${downloadTableName}"}_citation;

-- pre-create verbatim table so it can be used in the multi-insert
CREATE TABLE ${r"${downloadTableName}"} ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("serialization.null.format"="")
AS SELECT
<#list fields as field>
  ${field.hiveField}<#if field_has_next>,</#if>
</#list>
FROM ${r"${tableName}"}
WHERE ${r"${whereClause}"};

-- creates the citations table, citation table is not compressed since it is read later from Java as TSV.
SET mapred.output.compress=false;
SET hive.exec.compress.output=false;
SET mapred.reduce.tasks=1;

-- See https://github.com/gbif/occurrence/issues/28#issuecomment-432958372
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

CREATE TABLE ${r"${downloadTableName}"}_citation ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT datasetkey, count(*) as num_occurrences, license
FROM ${r"${downloadTableName}"}
WHERE datasetkey IS NOT NULL
GROUP BY datasetkey, license;
