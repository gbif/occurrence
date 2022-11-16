<#list extensions as extension>
DROP TABLE IF EXISTS ${r"${tableName}"}_ext_${extension.hiveTableName}_avro;
DROP TABLE IF EXISTS ${r"${tableName}"}_ext_${extension.hiveTableName};
</#list>
