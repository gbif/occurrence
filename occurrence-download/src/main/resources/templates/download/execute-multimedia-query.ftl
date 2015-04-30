<#-- Required syntax to escape Hive parameters. Outputs "USE ${hiveDB};" -->
USE ${r"${hiveDB}"};

CREATE TEMPORARY FUNCTION contains AS 'org.gbif.occurrence.hive.udf.ContainsUDF';
CREATE TEMPORARY FUNCTION toISO8601 AS 'org.gbif.occurrence.hive.udf.ToISO8601UDF';
CREATE TEMPORARY FUNCTION cleanDelimiters AS 'org.gbif.occurrence.hive.udf.CleanDelimiterCharsUDF';
CREATE TEMPORARY FUNCTION fromJson AS 'brickhouse.udf.json.FromJsonUDF';
CREATE TEMPORARY FUNCTION joinArray AS 'brickhouse.udf.collect.JoinArrayUDF';

-- TODO: enable this when we're done testing the basics
-- setup for our custom, combinable deflated compression
-- SET hive.exec.compress.output=true;
-- SET io.seqfile.compression.type=BLOCK;
-- SET mapred.output.compression.codec=org.gbif.hadoop.compress.d2.D2Codec;
-- SET io.compression.codecs=org.gbif.hadoop.compress.d2.D2Codec;

-- in case this job is relaunched
DROP TABLE IF EXISTS ${r"${multimediaTable}"};


SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapred.map.tasks=1000;


-- Creates the multimedia table
-- These will be small tables, so provide reducer hint to MR, to stop is spawning huge numbers (1 per machine)
--
SET mapred.reduce.tasks=12;
CREATE TABLE ${r"${multimediaTable}"}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("serialization.null.format"="")
AS SELECT
  gbifid AS gbifId,
  cleanDelimiters(mm_record['type']) AS type,
  cleanDelimiters(mm_record['format']) AS format_,
  cleanDelimiters(mm_record['identifier']) AS identifier,
  cleanDelimiters(mm_record['references']) AS references,
  cleanDelimiters(mm_record['title']) AS title,
  cleanDelimiters(mm_record['description']) AS description,
  cleanDelimiters(mm_record['source']) as source,
  cleanDelimiters(mm_record['audience']) as audience,
  toISO8601(mm_record['created']) as created,
  cleanDelimiters(mm_record['creator']) as creator,
  cleanDelimiters(mm_record['contributor']) as contributor,
  cleanDelimiters(mm_record['publisher']) as publisher,
  cleanDelimiters(mm_record['license']) as license,
  cleanDelimiters(mm_record['rightsHolder']) as rightsholder
FROM (
  SELECT gbifid, ext_multimedia
  FROM occurrence_hdfs
  WHERE ${r"${whereClause}"}
) occ_mm
  LATERAL VIEW explode(fromJson(occ_mm.ext_multimedia, 'array<map<string,string>>')) x AS mm_record;
