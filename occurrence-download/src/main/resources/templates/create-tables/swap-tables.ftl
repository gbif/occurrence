USE ${r"${hiveDB}"};

-- Rename old tables
ALTER TABLE ${r"${coreTermName}"}_avro RENAME TO old_${r"${coreTermName}"}_avro;
ALTER TABLE ${r"${coreTermName}"} RENAME TO old_${r"${coreTermName}"};
ALTER TABLE ${r"${coreTermName}"}_multimedia RENAME TO old_${r"${coreTermName}"}_multimedia;


-- Rename new tables
ALTER TABLE new_${r"${coreTermName}"}_avro RENAME TO ${r"${coreTermName}"}_avro;
ALTER TABLE new_${r"${coreTermName}"} RENAME TO ${r"${coreTermName}"};
ALTER TABLE new_${r"${coreTermName}"}_multimedia RENAME TO ${r"${coreTermName}"}_multimedia;

<#list extensions as extension>
-- ${extension.extension} renaming
ALTER TABLE ${r"${coreTermName}"}_ext_${extension.hiveTableName}_avro RENAME TO old_${r"${coreTermName}"}_ext_${extension.hiveTableName}_avro;
ALTER TABLE ${r"${coreTermName}"}_ext_${extension.hiveTableName} RENAME TO old_${r"${coreTermName}"}_ext_${extension.hiveTableName};

ALTER TABLE new_${r"${coreTermName}"}_ext_${extension.hiveTableName}_avro RENAME TO ${r"${coreTermName}"}_ext_${extension.hiveTableName}_avro;
ALTER TABLE new_${r"${coreTermName}"}_ext_${extension.hiveTableName} RENAME TO ${r"${coreTermName}"}_ext_${extension.hiveTableName};
</#list>



