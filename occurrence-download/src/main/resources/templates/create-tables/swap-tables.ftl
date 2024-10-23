USE ${r"${hiveDB}"};

-- Rename old tables
ALTER TABLE ${r"${tableName}"} RENAME TO old_${r"${tableName}"};
ALTER TABLE ${r"${tableName}"}_multimedia RENAME TO old_${r"${tableName}"}_multimedia;

-- Rename new tables
ALTER TABLE new_${r"${tableName}"} RENAME TO ${r"${tableName}"};
ALTER TABLE new_${r"${tableName}"}_multimedia RENAME TO ${r"${tableName}"}_multimedia;

<#list extensions as extension>
-- ${extension.extension} renaming
ALTER TABLE ${r"${tableName}"}_ext_${extension.hiveTableName} RENAME TO old_${r"${tableName}"}_ext_${extension.hiveTableName};
ALTER TABLE new_${r"${tableName}"}_ext_${extension.hiveTableName} RENAME TO ${r"${tableName}"}_ext_${extension.hiveTableName};

</#list>
