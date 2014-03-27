CREATE TEMPORARY FUNCTION cleanNull AS 'org.gbif.occurrence.hive.udf.NullStringRemoverUDF';
CREATE TEMPORARY FUNCTION toISO8601 AS 'org.gbif.occurrence.hive.udf.ToISO8601UDF';
CREATE TEMPORARY FUNCTION cleanDelimiters AS 'org.gbif.occurrence.hive.udf.CleanDelimiterCharsUDF';

set mapred.output.compress=false;
set hive.exec.compress.output=false;

CREATE TABLE ${result_multimedia_table}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS
SELECT cleanNull(mm.gbifid),cleanDelimiters(mm.type),cleanDelimiters(mm.format_),cleanDelimiters(mm.identifier),cleanDelimiters(mm.references),cleanDelimiters(mm.title),cleanDelimiters(mm.description),cleanDelimiters(mm.source),cleanDelimiters(mm.audience),toISO8601(mm.created),cleanDelimiters(mm.creator),cleanDelimiters(mm.contributor),cleanDelimiters(mm.publisher),cleanDelimiters(mm.license),cleanDelimiters(mm.rightsHolder) FROM ${multimedia_table} mm JOIN ${occurrence_intepreted_table} occ ON mm.gbifid = occ.gbifid;
