<#-- Required syntax to escape Hive parameters. Outputs "USE ${hiveDB};" -->
USE ${r"${hiveDB}"};

CREATE TEMPORARY FUNCTION cleanNull AS 'org.gbif.occurrence.hive.udf.NullStringRemoverUDF';
CREATE TEMPORARY FUNCTION contains AS 'org.gbif.occurrence.hive.udf.ContainsUDF';
CREATE TEMPORARY FUNCTION toISO8601 AS 'org.gbif.occurrence.hive.udf.ToISO8601UDF';
CREATE TEMPORARY FUNCTION cleanDelimiters AS 'org.gbif.occurrence.hive.udf.CleanDelimiterCharsUDF';
CREATE TEMPORARY FUNCTION joinArray AS 'brickhouse.udf.collect.JoinArrayUDF';

-- TODO: enable this when we're done testing the basics
-- setup for our custom, combinable deflated compression
-- SET hive.exec.compress.output=true;
-- SET io.seqfile.compression.type=BLOCK;
-- SET mapred.output.compression.codec=org.gbif.hadoop.compress.d2.D2Codec;
-- SET io.compression.codecs=org.gbif.hadoop.compress.d2.D2Codec;

-- in case this job is relaunched
DROP TABLE IF EXISTS ${r"${interpretedTable}"};

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapred.map.tasks=1000;


CREATE TABLE ${r"${interpretedTable}"}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT
<#list interpretedFields as field>
  ${field.hiveField}<#if field_has_next>,</#if>
</#list>
FROM occurrence_download_simple
WHERE
  ${r"${whereClause}"};
