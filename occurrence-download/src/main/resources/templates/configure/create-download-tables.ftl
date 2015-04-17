<#--
  This is a freemarker template which will generate an HQL script.
  When run in Hive as a parameterized query, this will create a set of optimized tables which are used
  during downloads.
-->
<#-- Required syntax to escape Hive parameters. Outputs "USE ${hive_db};" -->
USE ${r"${hiveDB}"};

-- UDFs used for the multimedia table only
CREATE TEMPORARY FUNCTION cleanNull AS 'org.gbif.occurrence.hive.udf.NullStringRemoverUDF';
CREATE TEMPORARY FUNCTION toISO8601 AS 'org.gbif.occurrence.hive.udf.ToISO8601UDF';
CREATE TEMPORARY FUNCTION cleanDelimiters AS 'org.gbif.occurrence.hive.udf.CleanDelimiterCharsUDF';
CREATE TEMPORARY FUNCTION fromJson AS 'brickhouse.udf.json.FromJsonUDF';
CREATE TEMPORARY FUNCTION join_array AS 'brickhouse.udf.collect.JoinArrayUDF';


-- snappy compression
SET hive.exec.compress.output=true;
SET mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

--
-- Supports the full download
--
CREATE TABLE occurrence_download STORED AS RCFILE AS
SELECT
<#list fullDownloadFields as field>
  ${field}<#if field_has_next>,</#if>
</#list>
FROM occurrence_hdfs;

--
-- Supports the simple download
--
CREATE TABLE occurrence_download_simple STORED AS RCFILE AS
SELECT
<#list simpleDownloadFields as field>
  ${field}<#if field_has_next>,</#if>
</#list>
FROM occurrence_download;

--
-- Common to any download format needed multimedia
-- Note: this is not Java Term driven as it is simple and maintainable like this.  It does require that
-- the simple download have the ext_multimedia column.  Should that change, then this needs revisited.
-- We apply all UDFs here, as this is only joined by gbifId, and thus we can have untyped columns
--
CREATE TABLE occurrence_multimedia STORED AS RCFILE AS
SELECT
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
  SELECT occ.gbifid, occ.ext_multimedia FROM occurrence_hdfs occ
) occ_mm
LATERAL VIEW explode(fromJson(occ_mm.ext_multimedia, 'array<map<string,string>>')) x AS mm_record;
