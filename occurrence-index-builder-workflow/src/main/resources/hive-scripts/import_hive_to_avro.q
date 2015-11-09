CREATE TEMPORARY FUNCTION removeNulls AS 'org.gbif.occurrence.hive.udf.ArrayNullsRemoverGenericUDF';

INSERT OVERWRITE TABLE ${avroTable}
SELECT
  gbifid,
  datasetkey,
  institutioncode,
  collectioncode,
  catalognumber,
  recordedby,
  recordnumber,
  IF(lastinterpreted IS NOT NULL, from_unixtime(lastinterpreted,'yyyy-MM-dd\'T\'00:00:00\'Z\''),NULL),
  removeNulls(array(kingdomkey,phylumkey,classkey,orderkey,familykey,genuskey,subgenuskey,specieskey,taxonkey)), --taxon_key
  countrycode,
  continent,
  publishingcountry,
  decimallatitude,
  decimallongitude,
  if(COALESCE(decimallatitude,-1000) BETWEEN -90.0 AND 90.0 AND COALESCE(decimallongitude,-1000) BETWEEN -180.0 AND 180.0,concat(CAST(decimallatitude AS STRING),',',CAST(decimallongitude AS STRING)),""), --coordinate
  year,
  month,
  IF(eventdate IS NOT NULL, from_unixtime(eventdate,'yyyy-MM-dd\'T\'00:00:00\'Z\''),NULL),
  COALESCE(basisofrecord,"UNKNOWN"),
  typestatus,
  hasgeospatialissues, --geospatial_issue
  hascoordinate, --hascoordinate
  elevation,
  depth,
  establishmentmeans,
  occurrenceid,
  mediatype,
  issue,
  scientificname
FROM ${sourceOccurrenceTable} occ;
