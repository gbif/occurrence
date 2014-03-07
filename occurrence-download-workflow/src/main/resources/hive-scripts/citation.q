set mapred.output.compress=false;
set hive.exec.compress.output=false;

DROP TABLE IF EXISTS ${citation_table};
CREATE TABLE ${citation_table}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS
SELECT datasetkey, count(*) as num_occurrences FROM ${query_result_table} GROUP BY datasetkey;
