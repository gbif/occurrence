USE ${hiveDB};

-- setup for our custom, combinable deflated compression
SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK;
SET mapred.output.compression.codec=org.gbif.hadoop.compress.d2.D2Codec;
SET io.compression.codecs=org.gbif.hadoop.compress.d2.D2Codec;
SET hive.merge.mapfiles=false;
SET hive.merge.mapredfiles=false;

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

CREATE TABLE ${occurrenceTable}_citation ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT datasetkey, COUNT(*) AS num_occurrences, license FROM iceberg.${hiveDB}.occurrence WHERE ${whereClause} AND datasetkey IS NOT NULL GROUP BY datasetkey, license;
