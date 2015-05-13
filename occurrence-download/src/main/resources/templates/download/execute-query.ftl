<#--
  This is a freemarker template which will generate an HQL script which is run at download time.
  When run in Hive as a parameterized query, this will create a set of tables ...
  TODO: document when we actually know something accurate to write here...
-->
<#-- Required syntax to escape Hive parameters. Outputs "USE ${hiveDB};" -->
USE ${r"${hiveDB}"};

CREATE TEMPORARY FUNCTION contains AS 'org.gbif.occurrence.hive.udf.ContainsUDF';
CREATE TEMPORARY FUNCTION toISO8601 AS 'org.gbif.occurrence.hive.udf.ToISO8601UDF';
CREATE TEMPORARY FUNCTION cleanDelimiters AS 'org.gbif.occurrence.hive.udf.CleanDelimiterCharsUDF';
CREATE TEMPORARY FUNCTION joinArray AS 'brickhouse.udf.collect.JoinArrayUDF';


-- setup for our custom, combinable deflated compression
SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK;
SET mapred.output.compression.codec=org.gbif.hadoop.compress.d2.D2Codec;
SET io.compression.codecs=org.gbif.hadoop.compress.d2.D2Codec;

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
FROM occurrence_hdfs
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


--
-- Creates the multimedia table
-- These will be small tables, so provide reducer hint to MR, to stop is spawning huge numbers
--
SET mapred.reduce.tasks=5;
CREATE TABLE ${r"${multimediaTable}"}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT m.*
FROM
  ${r"${interpretedTable}"} i
  JOIN occurrence_multimedia m ON m.gbifId = i.gbifId;

--
-- Creates the citation table
-- At most this produces #datasets, so single reducer
-- creates the citations table, citation table is not compressed since it is read later
SET mapred.output.compress=false;
SET hive.exec.compress.output=false;
SET mapred.reduce.tasks=1;
CREATE TABLE ${r"${citationTable}"}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT datasetkey, count(*) as num_occurrences FROM ${r"${interpretedTable}"} GROUP BY datasetkey;
