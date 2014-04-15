USE ${hive_db};
CREATE TABLE IF NOT EXISTS occurrence_multimedia_hdfs (gbifid INT,ext_multimedia STRING) STORED AS RCFILE;
INSERT OVERWRITE TABLE occurrence_multimedia_hdfs SELECT gbifid,ext_multimedia  FROM occurrence_multimedia_hbase;
