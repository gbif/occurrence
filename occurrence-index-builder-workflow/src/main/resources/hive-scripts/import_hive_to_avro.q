INSERT OVERWRITE TABLE ${tempAvroTable} 
SELECT 
  key,
  COALESCE(dataset_key,""),
  COALESCE(institution_code,""),
  COALESCE(collection_code,""),
  COALESCE(catalogue_number,""),
  COALESCE(collector_name,""),
  COALESCE(record_number,""),
  COALESCE(last_crawled, CAST(-1 AS BIGINT)),
  array(COALESCE(i_kingdom_key,-1),COALESCE(i_phylum_key,-1),COALESCE(i_class_key,-1),COALESCE(i_order_key,-1),COALESCE(i_family_key,-1),COALESCE(i_genus_key,-1),COALESCE(i_species_key,-1),COALESCE(i_taxon_key,-1)), --taxon_key
  COALESCE(i_country,""),
  COALESCE(pub_country,""),
  COALESCE(i_latitude,-1000),
  COALESCE(i_longitude,-1000),
  if(COALESCE(i_latitude,-1000) BETWEEN -90.0 AND 90.0 AND COALESCE(i_longitude,-1000) BETWEEN -180.0 AND 180.0,concat(CAST(i_latitude AS STRING),',',CAST(i_longitude AS STRING)),""), --coordinate  
  COALESCE(i_year,-1),
  COALESCE(i_month,-1),
  COALESCE(i_event_date,CAST(-1 AS BIGINT)),
  COALESCE(i_basis_of_record,-1),
  COALESCE(i_geospatial_issue,0) > 0, --geospatial_issue as boolean
  COALESCE(i_latitude,-1000) != -1000 AND COALESCE(i_longitude,-1000) != -1000, --georeferenced
  COALESCE(i_altitude,-1000000),
  COALESCE(i_depth,-1000000)
FROM ${sourceOccurrenceTable}; 