USE ${hiveDB};

-- For public use, prefix the functions with "gbif_" to avoid present and future collisions with SQL keywords, e.g. CONTAINS.
CREATE TEMPORARY FUNCTION gbif_eeaCellCode AS 'org.gbif.occurrence.hive.udf.EeaCellCodeUDF';
CREATE TEMPORARY FUNCTION gbif_geoDistance AS 'org.gbif.occurrence.hive.udf.GeoDistanceUDF';
CREATE TEMPORARY FUNCTION gbif_joinArray AS 'brickhouse.udf.collect.JoinArrayUDF';
CREATE TEMPORARY FUNCTION gbif_toISO8601 AS 'org.gbif.occurrence.hive.udf.ToISO8601UDF';
CREATE TEMPORARY FUNCTION gbif_toLocalISO8601 AS 'org.gbif.occurrence.hive.udf.ToLocalISO8601UDF';
CREATE TEMPORARY FUNCTION gbif_within AS 'org.gbif.occurrence.hive.udf.ContainsUDF';

DROP TABLE IF EXISTS ${occurrenceTable}_citation;

-- creates the citations table, citation table is not compressed since it is read later from Java as TSV.
SET mapred.output.compress=false;
SET hive.exec.compress.output=false;
SET mapred.reduce.tasks=1;
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

CREATE TABLE ${occurrenceTable}_citation ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT datasetkey, COUNT(*) AS num_occurrences, license FROM occurrence WHERE ${whereClause} AND datasetkey IS NOT NULL GROUP BY datasetkey, license;
