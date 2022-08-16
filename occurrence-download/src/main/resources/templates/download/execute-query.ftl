<#--
  This is a freemarker template which will generate an HQL script which is run at download time.
  When run in Hive as a parameterized query, this will create a set of tables ...
-->
<#-- Required syntax to escape Hive parameters. Outputs "USE ${hiveDB};" -->
USE ${r"${hiveDB}"};

CREATE TEMPORARY FUNCTION contains AS 'org.gbif.occurrence.hive.udf.ContainsUDF';
CREATE TEMPORARY FUNCTION geoDistance AS 'org.gbif.occurrence.hive.udf.GeoDistanceUDF';
CREATE TEMPORARY FUNCTION toISO8601 AS 'org.gbif.occurrence.hive.udf.ToISO8601UDF';
CREATE TEMPORARY FUNCTION toLocalISO8601 AS 'org.gbif.occurrence.hive.udf.ToLocalISO8601UDF';
CREATE TEMPORARY FUNCTION cleanDelimiters AS 'org.gbif.occurrence.hive.udf.CleanDelimiterCharsUDF';
CREATE TEMPORARY FUNCTION cleanDelimitersArray AS 'org.gbif.occurrence.hive.udf.CleanDelimiterArraysUDF';
CREATE TEMPORARY FUNCTION joinArray AS 'brickhouse.udf.collect.JoinArrayUDF';
CREATE TEMPORARY FUNCTION stringArrayContains AS 'org.gbif.occurrence.hive.udf.StringArrayContainsGenericUDF';

-- don't run joins locally, else risk running out of memory
SET hive.auto.convert.join=false;

-- setup for our custom, combinable deflated compression
-- See https://github.com/gbif/occurrence/issues/28#issuecomment-432958372
SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK;
SET mapred.output.compression.codec=org.gbif.hadoop.compress.d2.D2Codec;
SET io.compression.codecs=org.gbif.hadoop.compress.d2.D2Codec;
SET hive.merge.mapfiles=false;
SET hive.merge.mapredfiles=false;


-- in case this job is relaunched
DROP TABLE IF EXISTS ${r"${verbatimTable}"};
DROP TABLE IF EXISTS ${r"${interpretedTable}"};
DROP TABLE IF EXISTS ${r"${citationTable}"};
DROP TABLE IF EXISTS ${r"${multimediaTable}"};

-- pre-create verbatim table so it can be used in the multi-insert
CREATE TABLE ${r"${verbatimTable}"} (
<#list verbatimFields as field>
  ${field.hiveField} ${field.hiveDataType}<#if field_has_next>,</#if>
</#list>
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' TBLPROPERTIES ("serialization.null.format"="");

-- pre-create interpreted table so it can be used in the multi-insert
CREATE TABLE ${r"${interpretedTable}"} (
<#list interpretedFields as field>
  ${field.hiveField} ${field.hiveDataType}<#if field_has_next>,</#if>
</#list>
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' TBLPROPERTIES ("serialization.null.format"="");

--
-- Uses multi-table inserts format to reduce to a single scan of the source table.
--
<#-- NOTE: Formatted below to generate nice output at expense of ugliness in this template -->
FROM ${r"${tableName}"}
  INSERT INTO TABLE ${r"${verbatimTable}"}
  SELECT
<#list verbatimFields as field>
    ${field.hiveField}<#if field_has_next>,</#if>
</#list>
  WHERE ${r"${whereClause}"}
  INSERT INTO TABLE ${r"${interpretedTable}"}
  SELECT
<#list initializedInterpretedFields as field>
    ${field.hiveField}<#if field_has_next>,</#if>
</#list>
  WHERE ${r"${whereClause}"};


-- See https://github.com/gbif/occurrence/issues/28#issuecomment-432958372
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

--
-- Creates the multimedia table
-- These will be small tables, so provide reducer hint to MR, to stop is spawning huge numbers
--
SET mapred.reduce.tasks=5;
-- Disabling hive auto join https://issues.apache.org/jira/browse/HIVE-2601.
SET hive.auto.convert.join=false;
CREATE TABLE ${r"${multimediaTable}"} ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' TBLPROPERTIES ("serialization.null.format"="")
AS SELECT m.gbifid, m.type, m.format, m.identifier, m.references, m.title, m.description, m.source, m.audience, m.created, m.creator, m.contributor, m.publisher, m.license, m.rightsHolder
FROM ${r"${tableName}"}_multimedia m
JOIN ${r"${interpretedTable}"} i ON m.gbifId = i.gbifId;
SET hive.auto.convert.join=true;

--
-- Creates the citation table
-- At most this produces #datasets, so single reducer
-- creates the citations table, citation table is not compressed since it is read later from Java as TSV.
SET mapred.output.compress=false;
SET hive.exec.compress.output=false;
SET mapred.reduce.tasks=1;
CREATE TABLE ${r"${citationTable}"}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT datasetkey, count(*) as num_occurrences FROM ${r"${interpretedTable}"} WHERE datasetkey IS NOT NULL GROUP BY datasetkey;
