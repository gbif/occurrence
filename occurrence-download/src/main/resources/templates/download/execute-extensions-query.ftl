<#--
  This is a freemarker template which will generate an HQL script which is run at download time.
  When run in Hive as a parameterized query, this will create a set of tables ...
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
-- Creates the extension tables
-- These will be small tables, so provide reducer hint to MR, to stop is spawning huge numbers
--
SET mapred.reduce.tasks=5;
<#list verbatim_extensions as verbatim_extension>

-- ${verbatim_extension.extension} extension
CREATE TABLE IF NOT EXISTS ${downloadTableName}_ext_${verbatim_extension.hiveTableName}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' TBLPROPERTIES ("serialization.null.format"="")
AS SELECT ext.`${verbatim_extension.interpretedFields?join('`, ext.`')}`
FROM ${tableName}_ext_${verbatim_extension.hiveTableName} ext
JOIN ${interpretedTable} ON ${interpretedTable}.gbifid = ext.gbifid;
</#list>
