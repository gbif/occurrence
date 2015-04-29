<#-- @ftlvariable name="field" type="org.gbif.occurrence.download.hive.InitializableField" -->
<#--
  This is a freemarker template which will generate an HQL script.
  When run in Hive as a parameterized query, this will create an HDFS table which is a populated by running some
  a query over the HBase backed occurrence table.
-->

<#-- Required syntax to escape Hive parameters. Outputs "USE ${hiveDB};" -->
USE ${r"${hiveDB}"};

-- snappy compression
SET hive.exec.compress.output=true;
SET mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

-- configure for reading HBase
SET hbase.client.scanner.caching=1000;
SET hive.mapred.reduce.tasks.speculative.execution=false;
SET hive.hadoop.supports.splittable.combineinputformat=true;
SET mapred.max.split.size=256000000;

-- hint: ensure these are on the job classpath
CREATE TEMPORARY FUNCTION collectMediaTypes AS 'org.gbif.occurrence.hive.udf.CollectMediaTypesUDF';
CREATE TEMPORARY FUNCTION removeNulls AS 'org.gbif.occurrence.hive.udf.ArrayNullsRemoverGenericUDF';
CREATE TEMPORARY FUNCTION cleanDelimiters AS 'org.gbif.occurrence.hive.udf.CleanDelimiterCharsUDF';

-- create the HDFS view of the HBase table
CREATE TABLE IF NOT EXISTS occurrence_hdfs (
<#list fields as field>
  ${field.hiveField} ${field.hiveDataType}<#if field_has_next>,</#if>
</#list>
) STORED AS RCFILE;

-- populate the HDFS view
INSERT OVERWRITE TABLE occurrence_hdfs
SELECT
<#list fields as field>
  ${field.initializer}<#if field_has_next>,</#if>
</#list>
FROM occurrence_hbase;
