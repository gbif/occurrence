USE ${r"${hiveDB}"};

-- Rename old tables
ALTER TABLE ${r"${occurrenceTable}"}_avro RENAME TO old_${r"${occurrenceTable}"}_avro;
ALTER TABLE ${r"${occurrenceTable}"} RENAME TO old_${r"${occurrenceTable}"};
ALTER TABLE ${r"${occurrenceTable}"}_multimedia RENAME TO old_${r"${occurrenceTable}"}_multimedia;


-- Rename new tables
ALTER TABLE new_${r"${occurrenceTable}"}_avro RENAME TO ${r"${occurrenceTable}"}_avro;
ALTER TABLE new_${r"${occurrenceTable}"} RENAME TO ${r"${occurrenceTable}"};
ALTER TABLE new_${r"${occurrenceTable}"}_multimedia RENAME TO ${r"${occurrenceTable}"}_multimedia;

<#list extensions as extension>
-- ${extension.extension} renaming
ALTER TABLE ${r"${occurrenceTable}"}_ext_${extension.hiveTableName}_avro RENAME TO old_${r"${occurrenceTable}"}_ext_${extension.hiveTableName}_avro;
ALTER TABLE ${r"${occurrenceTable}"}_ext_${extension.hiveTableName} RENAME TO old_${r"${occurrenceTable}"}_ext_${extension.hiveTableName};

ALTER TABLE new_${r"${occurrenceTable}"}_ext_${extension.hiveTableName}_avro RENAME TO ${r"${occurrenceTable}"}_ext_${extension.hiveTableName}_avro;
ALTER TABLE new_${r"${occurrenceTable}"}_ext_${extension.hiveTableName} RENAME TO ${r"${occurrenceTable}"}_ext_${extension.hiveTableName};
</#list>



