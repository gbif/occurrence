USE ${hiveDB};

-- setup for our custom, combinable deflated compression
SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK;

CREATE TEMPORARY FUNCTION toISO8601 AS 'org.gbif.occurrence.hive.udf.ToISO8601UDF';
CREATE TEMPORARY FUNCTION contains AS 'org.gbif.occurrence.hive.udf.ContainsUDF';
CREATE TEMPORARY FUNCTION joinArray AS 'brickhouse.udf.collect.JoinArrayUDF';

DROP TABLE IF EXISTS ${occurrenceTable}_citation;

-- creates the citations table, citation table is not compressed since it is read later from Java as TSV.
SET mapred.output.compress=false;
SET hive.exec.compress.output=false;
SET mapred.reduce.tasks=1;
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

CREATE TABLE ${occurrenceTable}_citation ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT datasetkey, count(*) as num_occurrences, license FROM occurrence_pipeline_hdfs WHERE ${whereClause} AND datasetkey IS NOT NULL GROUP BY datasetkey, license;
