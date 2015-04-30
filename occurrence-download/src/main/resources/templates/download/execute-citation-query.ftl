<#-- Required syntax to escape Hive parameters. Outputs "USE ${hiveDB};" -->
USE ${r"${hiveDB}"};

-- in case this job is relaunched
DROP TABLE IF EXISTS ${r"${citationTable}"};


SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET mapred.map.tasks=1000;

--
-- Creates the citation table
-- At most this produces #datasets, so single reducer
--
SET mapred.reduce.tasks=1;
CREATE TABLE ${r"${citationTable}"}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT datasetkey, count(*) as num_occurrences
FROM occurrence_hdfs
WHERE
  ${r"${whereClause}"}
GROUP BY datasetkey;
