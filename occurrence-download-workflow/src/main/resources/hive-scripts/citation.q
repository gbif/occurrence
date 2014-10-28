SET mapred.output.compress=false;
SET hive.exec.compress.output=false;

SET io.compression.codecs=org.gbif.hadoop.compress.d2.D2Codec;

CREATE TABLE ${citation_table}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT datasetkey, count(*) as num_occurrences FROM ${query_result_table} GROUP BY datasetkey;
