<#-- @ftlvariable name="field" type="org.gbif.occurrence.download.hive.HBaseField" -->
<#--
  This is a freemarker template which will generate an HQL script.
  When run in Hive as a parameterized query, this will create a Hive table which is a
  backed by the HBase table.
-->

<#-- Required syntax to escape Hive parameters. Outputs "USE ${hive_db};" -->
USE ${r"${hiveDB}"};

-- create the HBase table view
DROP TABLE IF EXISTS occurrence_hbase;
CREATE EXTERNAL TABLE IF NOT EXISTS occurrence_hbase (
<#list fields as field>
  ${field.hiveField} ${field.hiveDataType}<#if field_has_next>,</#if>
</#list>
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = "
<#list fields as field>
  ${field.hbaseColumn}<#if field_has_next>,</#if>
</#list>
") TBLPROPERTIES(
  "hbase.table.name" = "${r"${occurrenceHBaseTable}"}",
  "hbase.table.default.storage.type" = "binary"
);
