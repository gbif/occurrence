<#--
  This is a freemarker template which will generate an HQL script which is run at download time.
  When run in Hive as a parameterized query, this will create a set of tables ...
-->
<#-- Required syntax to escape Hive parameters. Outputs "USE ${hiveDB};" -->
USE ${r"${hiveDB}"};

SET hive.merge.mapfiles=false;
-- Increases memory to avoid a "Container … is running beyond physical memory limits." error.
SET mapreduce.map.memory.mb=8192;

-- in case this job is relaunched
DROP TABLE IF EXISTS ${r"${downloadTableName}"};
DROP TABLE IF EXISTS ${r"${downloadTableName}"}_citation;

-- pre-create verbatim table so it can be used in the multi-insert
CREATE TABLE ${r"${downloadTableName}"} (
<#list parquetFields as key, field>
  `${field.hiveField}` ${field.hiveDataType}<#if key_has_next>,</#if>
</#list>
)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");

INSERT INTO ${r"${downloadTableName}"}
SELECT
<#list hiveFields as key, field>
  ${field.hiveField} AS ${parquetFields[key].hiveField}<#if key_has_next>,</#if>
</#list>
FROM ${r"${tableName}"}
WHERE ${r"${whereClause}"};

-- creates the citations table, citation table is not compressed since it is read later from Java as TSV.
SET mapred.output.compress=false;
SET hive.exec.compress.output=false;
SET spark.sql.shuffle.partitions=1;

-- See https://github.com/gbif/occurrence/issues/28#issuecomment-432958372
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

CREATE TABLE ${r"${downloadTableName}"}_citation ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT datasetkey, count(*) as num_occurrences, license
FROM ${r"${downloadTableName}"}
WHERE datasetkey IS NOT NULL
GROUP BY datasetkey, license;
