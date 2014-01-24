INSERT OVERWRITE TABLE ${tempAvroTable} 
SELECT 
  id,
  COALESCE(dataset_id,""),
  COALESCE(institution_code,""),
  COALESCE(collection_code,""),
  COALESCE(catalog_number,""),
  COALESCE(recorded_by,""),
  COALESCE(record_number,""),
  COALESCE(modified, CAST(-1 AS BIGINT)),
  array(COALESCE(kingdom_id,-1),COALESCE(phylum_id,-1),COALESCE(class_id,-1),COALESCE(order_id,-1),COALESCE(family_id,-1),COALESCE(genus_id,-1),COALESCE(species_id,-1),COALESCE(taxon_id,-1)), --taxon_key
  COALESCE(country_code,""),
  COALESCE(host_country,""),
  COALESCE(latitude,-1000),
  COALESCE(longitude,-1000),
  if(COALESCE(latitude,-1000) BETWEEN -90.0 AND 90.0 AND COALESCE(longitude,-1000) BETWEEN -180.0 AND 180.0,concat(CAST(latitude AS STRING),',',CAST(longitude AS STRING)),""), --coordinate  
  COALESCE(year,-1),
  COALESCE(month,-1),
  COALESCE(event_date,CAST(-1 AS BIGINT)),
  COALESCE(basis_of_record,-1),
  COALESCE(geospatial_issue,0) > 0, --geospatial_issue as boolean
  COALESCE(latitude,-1000) != -1000 AND COALESCE(longitude,-1000) != -1000, --georeferenced
  COALESCE(elevation_in_meters,-1000000),
  COALESCE(depth_in_meters,-1000000)
FROM ${sourceOccurrenceTable}; 