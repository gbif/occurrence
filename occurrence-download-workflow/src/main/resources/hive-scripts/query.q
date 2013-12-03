CREATE TEMPORARY FUNCTION cleanNull AS 'org.gbif.occurrencestore.hive.udf.NullStringRemoverUDF';
CREATE TEMPORARY FUNCTION getBorEnum AS 'org.gbif.occurrencestore.hive.udf.BasisOfRecordLookupUDF';
CREATE TEMPORARY FUNCTION contains AS 'org.gbif.occurrencestore.hive.udf.ContainsUDF';
CREATE TEMPORARY FUNCTION toISO8601 AS 'org.gbif.occurrencestore.hive.udf.ToISO8601UDF';
CREATE TEMPORARY FUNCTION cleanDelimiters AS 'org.gbif.occurrencestore.hive.udf.CleanDelimiterCharsUDF';

set mapred.output.compress=false;
set hive.exec.compress.output=false;
set hbase.client.scanner.caching=200;

CREATE TABLE ${query_result}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS
SELECT ${select}
FROM ${occurrence_record}
WHERE ${query};
