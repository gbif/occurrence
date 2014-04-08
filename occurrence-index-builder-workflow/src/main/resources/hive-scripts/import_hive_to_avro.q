INSERT OVERWRITE TABLE ${tempAvroTable} 
SELECT 
  gbifid,
  COALESCE(datasetkey,""),
  COALESCE(institutioncode,""),
  COALESCE(collectioncode,""),
  COALESCE(catalognumber,""),
  COALESCE(recordedby,""),
  COALESCE(recordnumber,""),
  COALESCE(lastinterpreted, CAST(-1 AS BIGINT)),
  array(COALESCE(kingdomkey,-1),COALESCE(phylumkey,-1),COALESCE(classkey,-1),COALESCE(orderkey,-1),COALESCE(familykey,-1),COALESCE(genuskey,-1),COALESCE(specieskey,-1),COALESCE(taxonkey,-1)), --taxon_key
  COALESCE(countrycode,""),
  COALESCE(continent,""),
  COALESCE(publishingcountry,""),
  COALESCE(decimallatitude,-1000),
  COALESCE(decimallongitude,-1000),
  if(COALESCE(decimallatitude,-1000) BETWEEN -90.0 AND 90.0 AND COALESCE(decimallongitude,-1000) BETWEEN -180.0 AND 180.0,concat(CAST(decimallatitude AS STRING),',',CAST(decimallongitude AS STRING)),""), --coordinate  
  COALESCE(year,-1),
  COALESCE(month,-1),
  COALESCE(eventdate,CAST(-1 AS BIGINT)),
  COALESCE(basisofrecord,"UNKNOWN"),
  COALESCE(typestatus,"-1"),
  hasgeospatialissues, --geospatial_issue
  hascoordinate, --hascoordinate
  COALESCE(elevation,-1000000),
  COALESCE(depth,-1000000),
  mediatype
FROM ${sourceOccurrenceTable} occ; 