set mapred.output.compress=false;
set hive.exec.compress.output=false;

DROP TABLE IF EXISTS ${citation};
CREATE TABLE ${citation}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS
SELECT dataset_id, count(*) as num_occurrences FROM ${query_result} GROUP BY dataset_id;
