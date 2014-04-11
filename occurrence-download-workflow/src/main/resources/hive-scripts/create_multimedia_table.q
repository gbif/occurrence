CREATE TEMPORARY FUNCTION cleanNull AS 'org.gbif.occurrence.hive.udf.NullStringRemoverUDF';
CREATE TEMPORARY FUNCTION toISO8601 AS 'org.gbif.occurrence.hive.udf.ToISO8601UDF';
CREATE TEMPORARY FUNCTION cleanDelimiters AS 'org.gbif.occurrence.hive.udf.CleanDelimiterCharsUDF';
CREATE TEMPORARY FUNCTION from_json AS 'brickhouse.udf.json.FromJsonUDF';

set mapred.output.compress=false;
set hive.exec.compress.output=false;

CREATE TABLE ${result_multimedia_table}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS

SELECT cleanNull(gbifid),cleanDelimiters(mm_record['type']),cleanDelimiters(mm_record['format']),cleanDelimiters(mm_record['identifier']),cleanDelimiters(mm_record['references']),cleanDelimiters(mm_record['title']),cleanDelimiters(mm_record['description']),cleanDelimiters(mm_record['source']),cleanDelimiters(mm_record['audience']),toISO8601(mm_record['created']),cleanDelimiters(mm_record['creator']),cleanDelimiters(mm_record['contributor']),cleanDelimiters(mm_record['publisher']),cleanDelimiters(mm_record['license']),cleanDelimiters(mm_record['rightsHolder']) 
FROM (
  SELECT occ.gbifid, mm.ext_multimedia  FROM ${multimedia_table} mm 
  JOIN ${occurrence_intepreted_table} occ ON occ.gbifid = mm.gbifid 
) occ_mm LATERAL VIEW explode(from_json(ext_multimedia, 'array<map<string,string>>')) x AS mm_record;