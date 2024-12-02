USE  ${r"${hiveDB}"};
DROP TABLE IF EXISTS ${r"${downloadTableName}"}_interpreted;
DROP TABLE IF EXISTS ${r"${downloadTableName}"}_verbatim;
DROP TABLE IF EXISTS ${r"${downloadTableName}"}_citation;
DROP TABLE IF EXISTS ${r"${downloadTableName}"}_multimedia;
<#list extensions as extension>
-- ${extension.extension} extension
DROP TABLE IF EXISTS ${r"${downloadTableName}"}_ext_${extension.hiveTableName};
</#list>


