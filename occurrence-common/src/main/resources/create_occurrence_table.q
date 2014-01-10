--
-- This script creates the occurrence table that both the download service and any adhoc queries will use. As such it
-- contains some fields that are not used by the download. Make sure to name the tables correctly - typically they'll
-- have a prefix like dev_ or uat_. The unused-by-download fields are:
-- - crawl_id
-- - xml_schema
-- - dwc_occurrence_id
-- - harvested_date
-- - data_provider_id
-- - data_resource_id
-- - resource_access_point_id
-- - occurrence_date
-- - unit_qualifier
-- - cell_id
-- - centi_cell_id
-- - mod360_cell_id
-- - taxonomic_issue
-- - geospatial_issue
-- - other_issue

DROP TABLE IF EXISTS occurrence;
CREATE EXTERNAL TABLE occurrence (
  id INT,
  dataset_id STRING,
  owning_org_id STRING,
  crawl_id INT,
  xml_schema STRING,
  protocol STRING,
  dwc_occurrence_id STRING,
  harvested_date BIGINT,
  data_provider_id INT,
  data_resource_id INT,
  resource_access_point_id INT,
  institution_code STRING,
  collection_code STRING,
  catalog_number STRING,
  verbatim_scientific_name STRING,
  scientific_name_author STRING,
  taxon_rank STRING,
  verbatim_kingdom STRING,
  verbatim_phylum STRING,
  verbatim_class STRING,
  verbatim_order STRING,
  verbatim_family STRING,
  verbatim_genus STRING,
  verbatim_specific_epithet STRING,
  verbatim_infraspecific_epithet STRING,
  verbatim_latitude STRING,
  verbatim_longitude STRING,
  coordinate_precision STRING,
  maximum_elevation_in_meters STRING,
  minimum_elevation_in_meters STRING,
  elevation_precision STRING,
  minimum_depth_in_meters STRING,
  maximum_depth_in_meters STRING,
  depth_precision STRING,
  continent_ocean STRING,
  country STRING,
  state_province STRING,
  county STRING,
  recorded_by STRING,
  locality STRING,
  verbatim_year STRING,
  verbatim_month STRING,
  day STRING,
  occurrence_date STRING,
  verbatim_basis_of_record STRING,
  identified_by STRING,
  date_identified BIGINT,
  unit_qualifier STRING,
  created BIGINT,
  modified BIGINT,
  host_country STRING,

  kingdom STRING,
  phylum STRING,
  class_ STRING,
  order_ STRING,
  family STRING,
  genus STRING,
  specific_epithet STRING,
  scientific_name STRING,
  kingdom_id INT,
  phylum_id INT,
  class_id INT,
  order_id INT,
  family_id INT,
  genus_id INT,
  species_id INT,
  taxon_id INT,
  country_code STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  cell_id INT,
  centi_cell_id INT,
  mod360_cell_id INT,
  year INT,
  month INT,
  event_date BIGINT,
  basis_of_record INT,
  taxonomic_issue INT,
  geospatial_issue INT,
  other_issue INT,
  elevation_in_meters INT,
  depth_in_meters INT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key#b,o:dk#b,o:ook#b,o:ci#b,o:xs#b,o:pr#s,o:doi#b,o:hd#b,o:dpi#b,o:dri#b,o:rapi#b,o:ic#b,o:cc#b,o:cn#b,o:sn#b,o:a#b,o:r#b,o:k#b,o:p#b,o:c#b,o:o#b,o:f#b,o:g#b,o:s#b,o:ss#b,o:lat#b,o:lng#b,o:llp#b,o:maxa#b,o:mina#b,o:ap#b,o:mind#b,o:maxd#b,o:dp#b,o:co#b,o:ctry#b,o:sp#b,o:cty#b,o:coln#b,o:loc#b,o:y#b,o:m#b,o:d#b,o:od#b,o:bor#b,o:idn#b,o:idd#b,o:uq#b,o:crtd#b,o:mod#b,o:hc#s,o:ik#s,o:ip#s,o:icl#s,o:io#s,o:if#s,o:ig#s,o:is#s,o:isn#s,o:iki#b,o:ipi#b,o:ici#b,o:ioi#b,o:ifi#b,o:igi#b,o:isi#b,o:ini#b,o:icc#s,o:ilat#b,o:ilng#b,o:icell#b,o:iccell#b,o:i360#b,o:iy#b,o:im#b,o:iod#b,o:ibor#b,o:itaxi#b,o:igeoi#b,o:iothi#b,o:ialt#b,o:idep#b")
TBLPROPERTIES(
  "hbase.table.name" = "occurrence",
  "hbase.table.default.storage.type" = "binary"
);
