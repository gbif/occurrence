USE ${hiveDB};

-- setup for our custom, combinable deflated compression
SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK;
SET mapred.output.compression.codec=org.gbif.hadoop.compress.d2.D2Codec;
SET io.compression.codecs=org.gbif.hadoop.compress.d2.D2Codec;
SET hive.merge.mapfiles=false;
SET hive.merge.mapredfiles=false;

CREATE TEMPORARY FUNCTION toISO8601 AS 'org.gbif.occurrence.hive.udf.ToISO8601UDF';
CREATE TEMPORARY FUNCTION contains AS 'org.gbif.occurrence.hive.udf.ContainsUDF';
CREATE TEMPORARY FUNCTION joinArray AS 'brickhouse.udf.collect.JoinArrayUDF';

-- in case this job is relaunched
DROP TABLE IF EXISTS ${occurrenceTable};

CREATE TABLE ${occurrenceTable} ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("serialization.null.format"="")
AS ${sql};

SET mapred.output.compress=false;
SET hive.exec.compress.output=false;
SET mapred.reduce.tasks=1;
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

CREATE TABLE ${occurrenceTable}_count AS SELECT count(*) FROM ${occurrenceTable};
