USE ${hiveDB};
DROP TABLE IF EXISTS ${downloadTableName}_interpreted;
DROP TABLE IF EXISTS ${downloadTableName}_verbatim;
DROP TABLE IF EXISTS ${downloadTableName}_citation;
DROP TABLE IF EXISTS ${downloadTableName}_multimedia;
<#list extensions as extension>
-- ${extension.extension} extension
DROP TABLE IF EXISTS ${r"${interpretedTable}"}_ext_${extension.hiveTableName};
</#list>

