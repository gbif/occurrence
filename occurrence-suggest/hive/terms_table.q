CREATE TABLE IF NOT EXISTS occurrence_terms (
  key STRING,
  term STRING,
  value STRING,
  weight FLOAT
) STORED AS ORC TBLPROPERTIES ("serialization.null.format"="","orc.compress.size"="65536","orc.compress"="ZLIB");

DROP TABLE IF EXISTS occurrence_terms;
CREATE TABLE occurrence_terms
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES (
'avro.schema.literal'='{
  "namespace":"org.gbif.api.model.occurrence",
  "name":"OccurrenceTerm",
  "type":"record",
  "fields":
    [{"name":"key","type":["string", "null"]},
    {"name":"term","type":["string", "null"]},
    {"name":"value","type":["string", "null"]},
    {"name":"weight","type":["float", "null"]}]
}');



INSERT OVERWRITE TABLE occurrence_terms
SELECT concat(term,'_',value), term, value, weight FROM (
SELECT "recorded_by" as term, recordedby as value , count(*) as weight FROM uat.occurrence_hdfs WHERE recordedby IS NOT NULL GROUP BY recordedby
UNION ALL
SELECT "collection_code" as term, collectioncode as value, count(*) as weight FROM uat.occurrence_hdfs WHERE collectioncode IS NOT NULL GROUP BY collectioncode
UNION ALL
SELECT "institution_code" as term, institutioncode as value, count(*) as weight FROM uat.occurrence_hdfs WHERE institutioncode IS NOT NULL GROUP BY institutioncode
UNION ALL
SELECT "record_number" as term, recordnumber as value, count(*) as weight FROM uat.occurrence_hdfs WHERE recordnumber IS NOT NULL GROUP BY recordnumber
UNION ALL
SELECT "organism_id" as term, organismid as value, count(*) as weight FROM uat.occurrence_hdfs WHERE organismid IS NOT NULL GROUP BY organismid
UNION ALL
SELECT "locality" as term, locality as value, count(*) as weight FROM uat.occurrence_hdfs WHERE locality IS NOT NULL GROUP BY locality
UNION ALL
SELECT "water_body" as term, waterbody as value, count(*) as weight FROM uat.occurrence_hdfs WHERE waterbody IS NOT NULL GROUP BY waterbody
UNION ALL
SELECT "state_province" as term, stateprovince as value, count(*) as weight FROM uat.occurrence_hdfs WHERE stateprovince IS NOT NULL GROUP BY stateprovince) terms;


SELECT "catalog_number" as term, catalognumber as value, count(*) as weight FROM uat.occurrence_hdfs WHERE catalognumber IS NOT NULL GROUP BY catalognumber
UNION ALL
