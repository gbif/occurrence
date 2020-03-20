<#--
  This is a freemarker template which will generate an HQL script.
  When run in Hive as a parameterized query, this will create a Hive table which is a
  backed by the HBase table.
-->

<#-- Required syntax to escape Hive parameters. Outputs "USE ${hive_db};" -->
USE ${r"${hiveDB}"};

-- Rename old tables
ALTER TABLE ${r"${occurrenceTable}"}_avro RENAME TO ${r"${occurrenceTable}"}_avro_old;
ALTER TABLE ${r"${occurrenceTable}"}_hdfs RENAME TO ${r"${occurrenceTable}"}_hdfs_old;
ALTER TABLE ${r"${occurrenceTable}"}_multimedia RENAME TO ${r"${occurrenceTable}"}_multimedia_old;


-- Rename new tables
ALTER TABLE ${r"${occurrenceTable}"}_new_avro RENAME TO ${r"${occurrenceTable}"}_avro;
ALTER TABLE ${r"${occurrenceTable}"}_new_hdfs RENAME TO ${r"${occurrenceTable}"}_hdfs;
ALTER TABLE ${r"${occurrenceTable}"}_new_multimedia RENAME TO ${r"${occurrenceTable}"}_multimedia;



