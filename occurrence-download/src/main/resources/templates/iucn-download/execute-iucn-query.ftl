<#--
  This is a freemarker template which will generate an HQL script which is run at download time.
  When run in Hive as a parameterized query, this will create a set of tables ...
-->
<#-- Required syntax to escape Hive parameters. Outputs "USE ${hiveDB};" -->
USE ${r"${hiveDB}"};

CREATE TEMPORARY FUNCTION toISO8601 AS 'org.gbif.occurrence.hive.udf.ToISO8601UDF';
CREATE TEMPORARY FUNCTION toLocalISO8601 AS 'org.gbif.occurrence.hive.udf.ToLocalISO8601UDF';

-- in case this job is relaunched
DROP TABLE IF EXISTS ${r"${occurrenceTable}"};
DROP TABLE IF EXISTS ${r"${occurrenceTable}"}_citation;
-- set Deflate Avro compression, the multiple blocks will later be combined without re-compressing
SET hive.exec.compress.output=true;
SET hive.exec.compress.intermediate=true;
SET avro.output.codec=deflate;
SET avro.mapred.deflate.level=9;

CREATE TABLE ${r"${occurrenceTable}"}
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
-- The schema could be programatically generated, but it seems useful to have something in the codebase to refer to.
TBLPROPERTIES ('avro.schema.url'='${r"${wfPath}"}/map-of-life.avsc');

INSERT INTO ${r"${occurrenceTable}"}
SELECT
  ${interpretedFields.gbifID.initializer},
  ${internalFields.publishingOrgKey.initializer},
  ${interpretedFields.datasetKey.initializer},
  ${interpretedFields.occurrenceID.initializer},

  ${interpretedFields.taxonomicStatus.initializer},
  ${interpretedFields.kingdom.initializer},
  ${interpretedFields.phylum.initializer},
  ${interpretedFields.class.initializer},
  ${interpretedFields.order.initializer},
  ${interpretedFields.family.initializer},
  ${interpretedFields.genus.initializer},
  ${interpretedFields.subgenus.initializer},
  ${interpretedFields.species.initializer},
  ${interpretedFields.specificEpithet.initializer},
  ${interpretedFields.infraspecificEpithet.initializer},
  ${interpretedFields.taxonRank.initializer},
  ${interpretedFields.scientificName.initializer},
  ${verbatimFields.v_verbatimTaxonRank.initializer},
  ${verbatimFields.v_scientificName.initializer},
  ${verbatimFields.v_scientificNameAuthorship.initializer},
  ${verbatimFields.v_vernacularName.initializer},

  ${interpretedFields.taxonKey.initializer},
  ${interpretedFields.speciesKey.initializer},

  ${interpretedFields.countryCode.initializer},
  ${interpretedFields.locality.initializer},
  ${interpretedFields.stateProvince.initializer},
  ${interpretedFields.occurrenceStatus.initializer},
  ${interpretedFields.individualCount.initializer},

  ${interpretedFields.hasCoordinate.initializer},
  ${interpretedFields.decimalLatitude.initializer},
  ${interpretedFields.decimalLongitude.initializer},
  ${interpretedFields.coordinateUncertaintyInMeters.initializer},
  ${interpretedFields.coordinatePrecision.initializer},
  ${interpretedFields.elevation.initializer},
  ${interpretedFields.elevationAccuracy.initializer},
  ${interpretedFields.depth.initializer},
  ${interpretedFields.depthAccuracy.initializer},
  ${interpretedFields.hasGeospatialIssues.initializer},
  ${verbatimFields.v_verbatimCoordinateSystem.initializer},
  ${verbatimFields.v_verbatimElevation.initializer},
  ${verbatimFields.v_verbatimDepth.initializer},
  ${verbatimFields.v_verbatimLocality.initializer},
  ${verbatimFields.v_verbatimSRS.initializer},

  ${interpretedFields.eventDate.initializer},
  ${interpretedFields.eventTime.initializer},
  ${interpretedFields.day.initializer},
  ${interpretedFields.month.initializer},
  ${interpretedFields.year.initializer},
  ${verbatimFields.v_verbatimEventDate.initializer},

  ${interpretedFields.basisOfRecord.initializer},
  ${interpretedFields.institutionCode.initializer},
  ${interpretedFields.collectionCode.initializer},
  ${interpretedFields.catalogNumber.initializer},
  ${interpretedFields.recordNumber.initializer},

  ${interpretedFields.identifiedBy.initializer},
  ${interpretedFields.dateIdentified.initializer},

  ${interpretedFields.license.initializer},
  ${interpretedFields.rightsHolder.initializer},
  ${interpretedFields.recordedBy.initializer},

  ${interpretedFields.typeStatus.initializer},
  ${interpretedFields.establishmentMeans.initializer},

  ${interpretedFields.sampleSizeUnit.initializer},
  ${interpretedFields.sampleSizeValue.initializer},
  ${interpretedFields.samplingEffort.initializer},
  ${interpretedFields.samplingProtocol.initializer},

  ${interpretedFields.lastInterpreted.initializer},
  ${interpretedFields.mediaType.initializer},
  ${interpretedFields.issue.initializer}
FROM occurrence_hdfs
WHERE ${r"${whereClause}"};

-- creates the citations table, citation table is not compressed since it is read later from Java as TSV.
SET mapred.output.compress=false;
SET hive.exec.compress.intermediate=false;
SET hive.exec.compress.output=false;
CREATE TABLE ${r"${occurrenceTable}"}_citation
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT datasetkey, count(*) as num_occurrences, license FROM ${r"${occurrenceTable}"} WHERE datasetkey IS NOT NULL GROUP BY datasetkey, license;
