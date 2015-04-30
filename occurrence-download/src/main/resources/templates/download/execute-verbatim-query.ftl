<#-- Required syntax to escape Hive parameters. Outputs "USE ${hiveDB};" -->
USE ${r"${hiveDB}"};

-- TODO: enable this when we're done testing the basics
-- setup for our custom, combinable deflated compression
-- SET hive.exec.compress.output=true;
-- SET io.seqfile.compression.type=BLOCK;
-- SET mapred.output.compression.codec=org.gbif.hadoop.compress.d2.D2Codec;
-- SET io.compression.codecs=org.gbif.hadoop.compress.d2.D2Codec;

-- in case this job is relaunched
DROP TABLE IF EXISTS ${r"${verbatimTable}"};

SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapred.map.tasks=1000;


-- pre-create verbatim table so it can be used in the multi-insert
CREATE TABLE ${r"${verbatimTable}"}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("serialization.null.format"="")
AS SELECT
<#list verbatimFields as field>
  ${field.hiveField}<#if field_has_next>,</#if>
</#list>
FROM occurrence_download_simple
WHERE
  ${r"${whereClause}"};
