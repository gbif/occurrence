DROP TABLE IF EXISTS ${tempAvroTable};
CREATE TABLE ${tempAvroTable}
ROW FORMAT SERDE 
'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 
'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 
'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES (
'avro.schema.literal'='{
  "namespace":"org.gbif.api.model.occurrence",
  "name":"Occurrence",
  "type":"record",
  "fields": 
    [{"name":"key","type":"int"},
    {"name":"dataset_key","type":"string"},
    {"name":"institution_code","type":"string"},
    {"name":"collection_code","type":"string"},
    {"name":"catalog_number","type":"string"}, 
    {"name":"recorded_by","type":"string"}, 
    {"name":"record_number","type":"string"},     
    {"name":"last_interpreted","type":"long"}, 
    {"name":"taxon_key","type":{"type":"array", "items":"int"}},
    {"name":"country","type":"string"},
    {"name":"continent","type":"string"},
    {"name":"publishing_country","type":"string"},
    {"name":"latitude","type":"double"},
    {"name":"longitude","type":"double"},
    {"name":"coordinate","type":"string"},
    {"name":"year","type":"int"},
    {"name":"month","type":"int"},
    {"name":"date","type":"long"},
    {"name":"basis_of_record","type":"string"},
    {"name":"type_status","type":"string"},
    {"name":"spatial_issues","type":"boolean"},
    {"name":"has_coordinate","type":"boolean"},
    {"name":"elevation","type":"int"},
    {"name":"depth","type":"int"}]
}');