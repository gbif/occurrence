USE  ${r"${hiveDB}"};
DROP TABLE IF EXISTS ${r"${downloadTableName}"}_interpreted PURGE;
DROP TABLE IF EXISTS ${r"${downloadTableName}"}_verbatim PURGE;
DROP TABLE IF EXISTS ${r"${downloadTableName}"}_citation PURGE;
DROP TABLE IF EXISTS ${r"${downloadTableName}"}_multimedia PURGE;
<#list extensions as extension>
-- ${extension.extension} extension
DROP TABLE IF EXISTS ${r"${downloadTableName}"}_ext_${extension.hiveTableName};
</#list>


