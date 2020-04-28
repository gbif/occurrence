USE ${hiveDB};

CREATE TEMPORARY FUNCTION toLocalISO8601 AS 'org.gbif.occurrence.hive.udf.ToLocalISO8601UDF';
CREATE TEMPORARY FUNCTION joinArray AS 'brickhouse.udf.collect.JoinArrayUDF';

-- creating the very wide results requires using Spark
--SET hive.execution.engine=spark;

-- don't run joins locally, else risk running out of memory
SET hive.auto.convert.join=false;

-- setup for our custom, combinable deflated compression
-- See https://github.com/gbif/occurrence/issues/28#issuecomment-432958372
SET hive.exec.compress.output=false;
-- SET io.seqfile.compression.type=BLOCK;
-- SET mapred.output.compression.codec=org.gbif.hadoop.compress.d2.D2Codec;
-- SET io.compression.codecs=org.gbif.hadoop.compress.d2.D2Codec;
SET hive.merge.mapfiles=false;
SET hive.merge.mapredfiles=false;

-- in case this job is relaunched
DROP TABLE IF EXISTS ${occurrenceTable};
DROP TABLE IF EXISTS ${occurrenceTable}_agents;
DROP TABLE IF EXISTS ${occurrenceTable}_families;
DROP TABLE IF EXISTS ${occurrenceTable}_citation;

-- datasetKey and license are required for calculating the citation.
CREATE TABLE dl_${occurrenceTable}
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
AS SELECT
  gbifID,
  datasetKey,
  license,
  countryCode,
  CAST(toLocalISO8601(dateidentified) AS BIGINT) AS dateIdentified,
  CAST(toLocalISO8601(eventdate) AS BIGINT) AS eventDate,
  array_contains(mediaType, 'StillImage') AS hasImage,
  v_occurrenceID,
  v_dateIdentified,
  v_decimalLatitude,
  v_decimalLongitude,
  v_country,
  v_eventDate,
  v_year,
  v_family,
  v_identifiedBy,
  v_identifiedByID,
  v_institutionCode,
  v_collectionCode,
  v_catalogNumber,
  v_recordedBy,
  v_recordedByID,
  v_scientificName,
  v_typeStatus
FROM occurrence_hdfs
WHERE (v_identifiedBy IS NOT NULL OR v_recordedBy IS NOT NULL)
  AND ${whereClause};

-- NB! If changing the columns, remember to update the header row in the workflow definition.
CREATE TABLE dl_${occurrenceTable}_agents
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  TBLPROPERTIES ("serialization.null.format"="")
AS SELECT
  COALESCE(r.agent, i.agent) AS agent,
  r.total_recordedBy,
  i.total_identifiedBy,
  if(r.gbifIDs_recordedBy IS NULL, NULL, joinArray(r.gbifIDs_recordedBy, '\\;')) AS gbifIDs_recordedBy,
  if(i.gbifIDs_identifiedBy IS NULL, NULL, joinArray(i.gbifIDs_identifiedBy, '\\;')) AS gbifIDs_identifiedBy
FROM
  (SELECT
     v_recordedBy AS agent,
     COUNT(gbifID) AS total_recordedBy,
     collect_set(CAST(gbifID AS STRING)) AS gbifIDs_recordedBy
  FROM dl_${occurrenceTable}
  WHERE v_recordedby IS NOT NULL
  GROUP BY v_recordedby) AS r
FULL OUTER JOIN
  (SELECT
     v_identifiedBy AS agent,
     COUNT(gbifID) AS total_identifiedBy,
     collect_set(CAST(gbifID AS STRING)) AS gbifIDs_identifiedBy
  FROM dl_${occurrenceTable}
  WHERE v_identifiedBy IS NOT NULL
  GROUP BY v_identifiedBy) AS i
ON r.agent = i.agent;

-- NB! If changing the columns, remember to update the header row in the workflow definition.
CREATE TABLE dl_${occurrenceTable}_families ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT
  v_family,
  COUNT(gbifID) total,
  joinArray(collect_set(CAST(gbifID AS STRING)), '\\;') AS gbifIDs_family
FROM dl_${occurrenceTable}
  WHERE v_family IS NOT NULL
  GROUP BY v_family;

-- creates the citations table, citation table is not compressed since it is read later from Java as TSV.
SET mapred.output.compress=false;
SET hive.exec.compress.intermediate=false;
SET hive.exec.compress.output=false;
CREATE TABLE ${occurrenceTable}_citation
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT datasetkey, count(*) as num_occurrences, license FROM dl_${occurrenceTable} WHERE datasetkey IS NOT NULL GROUP BY datasetkey, license;
