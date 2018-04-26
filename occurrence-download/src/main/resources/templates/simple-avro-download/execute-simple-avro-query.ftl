<#--
  This is a freemarker template which will generate an HQL script which is run at download time.
  When run in Hive as a parameterized query, this will create a set of tables ...
  TODO: document when we actually know something accurate to write here...
-->
<#-- Required syntax to escape Hive parameters. Outputs "USE ${hiveDB};" -->
USE ${r"${hiveDB}"};

CREATE TEMPORARY FUNCTION toISO8601 AS 'org.gbif.occurrence.hive.udf.ToISO8601UDF';
CREATE TEMPORARY FUNCTION contains AS 'org.gbif.occurrence.hive.udf.ContainsUDF';
CREATE TEMPORARY FUNCTION joinArray AS 'brickhouse.udf.collect.JoinArrayUDF';

-- in case this job is relaunched
DROP TABLE IF EXISTS ${r"${occurrenceTable}"};
DROP TABLE IF EXISTS ${r"${occurrenceTable}"}_citation;

-- set Deflate Avro compression, the multiple blocks will later be combined without re-compressing
SET hive.exec.compress.output=true;
SET hive.exec.compress.intermediate=true;
SET avro.output.codec=deflate;

CREATE TABLE ${r"${occurrenceTable}"}
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES ('avro.schema.url'='hdfs://ha-nn/occurrence-download-workflows-dev/download-workflow/simple-avro/occurrence.avsc');

INSERT INTO ${r"${occurrenceTable}"}
SELECT
<#list fields as field>
  ${field.hiveField}<#if field_has_next>,</#if>
</#list>
FROM occurrence_hdfs
WHERE ${r"${whereClause}"};

-- creates the citations table
CREATE TABLE ${r"${occurrenceTable}"}_citation
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT datasetkey, count(*) as num_occurrences FROM ${r"${occurrenceTable}"} WHERE datasetkey IS NOT NULL GROUP BY datasetkey;
