CREATE TEMPORARY FUNCTION removeNulls AS 'org.gbif.occurrence.hive.udf.ArrayNullsRemoverGenericUDF';

INSERT OVERWRITE TABLE ${r"${avroTable}"}
SELECT
gbifid,
datasetkey,
installationkey,
networkkey,
institutioncode,
collectioncode,
catalognumber,
recordedby,
recordnumber,
IF(lastinterpreted IS NOT NULL, concat(to_date(from_utc_timestamp(lastinterpreted,'UTC')),'T00:00:00Z'),NULL) AS lastinterpreted,
removeNulls(array(kingdomkey,phylumkey,classkey,orderkey,familykey,genuskey,subgenuskey,specieskey,taxonkey)) AS taxonkey, --taxon_key
acceptedtaxonkey,
kingdomkey,
phylumkey,
classkey,
orderkey,
familykey,
genuskey,
subgenuskey,
specieskey,
taxonomicstatus,
countrycode,
continent,
publishingcountry,
decimallatitude,
decimallongitude,
if(COALESCE(decimallatitude,-1000) BETWEEN -90.0 AND 90.0 AND COALESCE(decimallongitude,-1000) BETWEEN -180.0 AND 180.0,concat(CAST(decimallatitude AS STRING),',',CAST(decimallongitude AS STRING)),NULL) AS coordinate, --coordinate
year,
month,
IF(eventdate IS NOT NULL, concat(to_date(from_utc_timestamp(eventdate,'UTC')),'T00:00:00Z'),NULL) AS eventdate,
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
${field}<#if field_has_next>,</#if>
</#list>)),
repatriated,
locality,
organismid,
stateprovince,
waterbody,
protocol,
COALESCE(license,"UNSPECIFIED") AS license,
crawlid,
publishingorgkey,
eventid,
parenteventid,
samplingprotocol
FROM ${r"${sourceOccurrenceTable}"} occ;
