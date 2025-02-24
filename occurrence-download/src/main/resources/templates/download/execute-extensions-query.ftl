<#--
  This is a freemarker template which will generate an HQL script which is run at download time.
  When run in Hive as a parameterized query, this will create a set of tables ...

  Formatted below to generate nice output at expense of ugliness in this template.
-->
<#-- Required syntax to escape Hive parameters. Outputs "USE ${hiveDB};" -->
USE ${r"${hiveDB}"};

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

--
-- Create the download gbifId join table

CACHE TABLE ${downloadTableName}_occurrence_gbifId
SELECT gbifid FROM ${interpretedTable};

--
-- Creates the extension tables

<#list verbatim_extensions as verbatim_extension>
-- ${verbatim_extension.extension} extension

CREATE TABLE IF NOT EXISTS ${downloadTableName}_ext_${verbatim_extension.hiveTableName} (
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  TBLPROPERTIES ("serialization.null.format"="")
  AS
  SELECT /*+ REBALANCE */
    ext.gbifid,
  <#list verbatim_extension.interpretedFields as field>
    ext.${field} string<#if field_has_next>,</#if>
  </#list>
  FROM ${downloadTableName}_occurrence_gbifId f
  JOIN occurrence_ext_${verbatim_extension.hiveTableName} ext
  ON f.gbifid = ext.gbifid
  WHERE ext.gbifid IS NOT NULL;
</#list>
