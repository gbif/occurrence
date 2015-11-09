DROP TABLE IF EXISTS ${avroTable};
CREATE TABLE ${avroTable}
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
    [{"name":"key","type":["int", "null"]},
    {"name":"dataset_key","type":["string", "null"]},
    {"name":"institution_code","type":["string", "null"]},
    {"name":"collection_code","type":["string", "null"]},
    {"name":"catalog_number","type":["string", "null"]},
    {"name":"recorded_by","type":["string", "null"]},
    {"name":"record_number","type":["string", "null"]},
    {"name":"last_interpreted","type":["string", "null"]},
    {"name":"taxon_key","type":["null",{"type":"array", "items":"int"}],"default":null},
    {"name":"country","type":["string", "null"]},
    {"name":"continent","type":["string", "null"]},
    {"name":"publishing_country","type":["string", "null"]},
    {"name":"latitude","type":"double"},
    {"name":"longitude","type":"double"},
    {"name":"coordinate","type":["string", "null"]},
    {"name":"year","type":["int", "null"]},
    {"name":"month","type":["int", "null"]},
    {"name":"event_date","type":["string", "null"]},
    {"name":"basis_of_record","type":["string", "null"]},
    {"name":"type_status","type":["string", "null"]},
    {"name":"spatial_issues","type":["boolean", "null"]},
    {"name":"has_coordinate","type":["boolean", "null"]},
    {"name":"elevation","type":["int", "null"]},
    {"name":"depth","type":["int", "null"]},
    {"name":"establishment_means","type":["string", "null"]},
    {"name":"occurrence_id","type":["string", "null"]},
    {"name":"media_type","type":["null",{"type":"array", "items":"string"}],"default":null},
    {"name":"issue","type":["null",{"type":"array", "items":"string"}],"default":null},
    {"name":"scientific_name","type":["string", "null"]}]
}');
