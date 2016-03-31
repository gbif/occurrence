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
  IF(lastinterpreted IS NOT NULL, from_unixtime(lastinterpreted,'yyyy-MM-dd\'T\'00:00:00\'Z\''),NULL) AS lastinterpreted,
  removeNulls(array(kingdomkey,phylumkey,classkey,orderkey,familykey,genuskey,subgenuskey,specieskey,taxonkey)) AS taxonkey, --taxon_key
  countrycode,
  continent,
  publishingcountry,
  decimallatitude,
  decimallongitude,
  if(COALESCE(decimallatitude,-1000) BETWEEN -90.0 AND 90.0 AND COALESCE(decimallongitude,-1000) BETWEEN -180.0 AND 180.0,concat(CAST(decimallatitude AS STRING),',',CAST(decimallongitude AS STRING)),NULL) AS coordinate, --coordinate
  year,
  month,
  IF(eventdate IS NOT NULL, from_unixtime(eventdate,'yyyy-MM-dd\'T\'00:00:00\'Z\''),NULL) AS eventdate,
  COALESCE(basisofrecord,"UNKNOWN") AS basisofrecord,
  typestatus,
  hasgeospatialissues, --geospatial_issue
  hascoordinate, --hascoordinate
  elevation,
  depth,
  establishmentmeans,
  occurrenceid,
  mediatype,
  issue,
  scientificname,
  removeNulls(array(
  <#list fields as field>
    ${field.initializer}<#if field_has_next>,</#if>
  </#list>))
FROM ${sourceOccurrenceTable} occ;
