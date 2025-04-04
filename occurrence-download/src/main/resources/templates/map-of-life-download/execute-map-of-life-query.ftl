<#--
  This is a freemarker template which will generate an HQL script which is run at download time.
  When run in Hive as a parameterized query, this will create a set of tables ...
-->
<#-- Required syntax to escape Hive parameters. Outputs "USE ${hiveDB};" -->
USE ${r"${hiveDB}"};

-- in case this job is relaunched
DROP TABLE IF EXISTS ${r"${downloadTableName}"};
DROP TABLE IF EXISTS ${r"${downloadTableName}"}_citation;
-- set Deflate Avro compression, the multiple blocks will later be combined without re-compressing
SET hive.exec.compress.output=true;
SET hive.exec.compress.intermediate=true;
SET avro.output.codec=deflate;
SET avro.mapred.deflate.level=9;

CREATE TABLE ${r"${downloadTableName}"}
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
-- The schema could be programatically generated, but it seems useful to have something in the codebase to refer to.
TBLPROPERTIES ('avro.schema.literal'='${mapOfLifeAvroSchema}');

INSERT INTO ${r"${downloadTableName}"}
SELECT
<#list fields as key, field>
  ${field.hiveField}<#if key_has_next>,</#if>
</#list>
FROM iceberg.${r"${hiveDB}"}.${r"${tableName}"}
WHERE ${r"${whereClause}"};

-- creates the citations table, citation table is not compressed since it is read later from Java as TSV.
SET mapred.output.compress=false;
SET hive.exec.compress.intermediate=false;
SET hive.exec.compress.output=false;
CREATE TABLE ${r"${downloadTableName}"}_citation
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT datasetkey, count(*) as num_occurrences, license FROM ${r"${downloadTableName}"} WHERE datasetkey IS NOT NULL GROUP BY datasetkey, license;
