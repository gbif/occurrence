USE ${r"${hiveDB}"};

-- Rename old tables
ALTER TABLE ${r"${occurrenceTable}"}_avro RENAME TO old_${r"${occurrenceTable}"}_avro;
ALTER TABLE ${r"${occurrenceTable}"}_hdfs RENAME TO old_${r"${occurrenceTable}"}_hdfs;
ALTER TABLE ${r"${occurrenceTable}"}_multimedia RENAME TO old_${r"${occurrenceTable}"}_multimedia;


-- Rename new tables
ALTER TABLE new_${r"${occurrenceTable}"}_avro RENAME TO ${r"${occurrenceTable}"}_avro;
ALTER TABLE new_${r"${occurrenceTable}"}_hdfs RENAME TO ${r"${occurrenceTable}"}_hdfs;
ALTER TABLE new_${r"${occurrenceTable}"}_multimedia RENAME TO ${r"${occurrenceTable}"}_multimedia;



