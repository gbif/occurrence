USE ${hiveDB};

CREATE TEMPORARY FUNCTION contains AS 'org.gbif.occurrence.hive.udf.ContainsUDF';
CREATE TEMPORARY FUNCTION toLocalISO8601 AS 'org.gbif.occurrence.hive.udf.ToLocalISO8601UDF';
CREATE TEMPORARY FUNCTION joinArray AS 'brickhouse.udf.collect.JoinArrayUDF';

-- don't run joins locally, else risk running out of memory
SET hive.auto.convert.join=false;

-- in case this job is relaunched
DROP TABLE IF EXISTS ${occurrenceTable};
DROP TABLE IF EXISTS ${occurrenceTable}_citation;
DROP TABLE IF EXISTS ${occurrenceTable}_agents;
DROP TABLE IF EXISTS ${occurrenceTable}_families;

-- datasetKey and license are required for calculating the citation.
CREATE TABLE ${occurrenceTable}
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES ('avro.schema.url'='${wfPath}/bionomia.avsc')
AS SELECT
  gbifID,
  datasetKey,
  license,
  countryCode,
  toLocalISO8601(dateidentified) AS dateIdentified,
  toLocalISO8601(eventdate) AS eventDate,
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

-- creates the citations table, citation table is not compressed since it is read later from Java as TSV.
SET mapred.output.compress=false;
SET hive.exec.compress.intermediate=false;
SET hive.exec.compress.output=false;
CREATE TABLE ${occurrenceTable}_citation
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT datasetkey, count(*) as num_occurrences, license FROM ${occurrenceTable} WHERE datasetkey IS NOT NULL GROUP BY datasetkey, license;

-- Increase memory to support the very wide collect.
SET mapreduce.reduce.memory.mb=${bionomiaReducerMemory};
SET mapreduce.reduce.java.opts=${bionomiaReducerOpts};

-- NB! If changing the columns, remember to update the header row in the workflow definition.
CREATE TABLE ${occurrenceTable}_agents
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  TBLPROPERTIES ('avro.schema.url'='${wfPath}/bionomia-agents.avsc')
AS SELECT
  COALESCE(r.agent, i.agent) AS agent,
  r.total_recordedBy,
  i.total_identifiedBy,
  r.gbifIDs_recordedBy,
  i.gbifIDs_identifiedBy
FROM
  (SELECT
     v_recordedBy AS agent,
     COUNT(gbifID) AS total_recordedBy,
     collect_set(CAST(gbifID AS STRING)) AS gbifIDs_recordedBy
  FROM ${occurrenceTable}
  WHERE v_recordedby IS NOT NULL
  GROUP BY v_recordedby) AS r
FULL OUTER JOIN
  (SELECT
     v_identifiedBy AS agent,
     COUNT(gbifID) AS total_identifiedBy,
     collect_set(CAST(gbifID AS STRING)) AS gbifIDs_identifiedBy
  FROM ${occurrenceTable}
  WHERE v_identifiedBy IS NOT NULL
  GROUP BY v_identifiedBy) AS i
ON r.agent = i.agent;

-- NB! If changing the columns, remember to update the header row in the workflow definition.
CREATE TABLE ${occurrenceTable}_families
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
  STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
  TBLPROPERTIES ('avro.schema.url'='${wfPath}/bionomia-families.avsc')
AS SELECT
  v_family,
  COUNT(gbifID) total,
  joinArray(collect_set(CAST(gbifID AS STRING)), '\\;') AS gbifIDs_family
FROM ${occurrenceTable}
  WHERE v_family IS NOT NULL
  GROUP BY v_family;
