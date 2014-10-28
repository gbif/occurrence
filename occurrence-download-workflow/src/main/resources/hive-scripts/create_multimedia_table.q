CREATE TEMPORARY FUNCTION cleanNull AS 'org.gbif.occurrence.hive.udf.NullStringRemoverUDF';
CREATE TEMPORARY FUNCTION toISO8601 AS 'org.gbif.occurrence.hive.udf.ToISO8601UDF';
CREATE TEMPORARY FUNCTION cleanDelimiters AS 'org.gbif.occurrence.hive.udf.CleanDelimiterCharsUDF';
CREATE TEMPORARY FUNCTION from_json AS 'brickhouse.udf.json.FromJsonUDF';

SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK;
SET mapred.output.compression.codec=org.gbif.hadoop.compress.d2.D2Codec;
SET io.compression.codecs=org.gbif.hadoop.compress.d2.D2Codec;


CREATE TABLE ${result_multimedia_table}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS
SELECT cleanNull(gbifid),cleanDelimiters(mm_record['type']),cleanDelimiters(mm_record['format']),cleanDelimiters(mm_record['identifier']),cleanDelimiters(mm_record['references']),cleanDelimiters(mm_record['title']),cleanDelimiters(mm_record['description']),cleanDelimiters(mm_record['source']),cleanDelimiters(mm_record['audience']),toISO8601(mm_record['created']),cleanDelimiters(mm_record['creator']),cleanDelimiters(mm_record['contributor']),cleanDelimiters(mm_record['publisher']),cleanDelimiters(mm_record['license']),cleanDelimiters(mm_record['rightsHolder'])
FROM (
  SELECT occ.gbifid, occ.ext_multimedia  FROM ${occurrence_record} occ
  JOIN ${occurrence_intepreted_table} intocc ON intocc.gbifid = occ.gbifid
) occ_mm LATERAL VIEW explode(from_json(occ_mm.ext_multimedia, 'array<map<string,string>>')) x AS mm_record;
