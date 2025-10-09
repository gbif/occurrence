<#--
  This is a freemarker template which will generate an HQL script which is run at download time.
  When run in Hive as a parameterized query, this will create a set of tables ...
-->
<#-- Required syntax to escape Hive parameters. Outputs "USE ${hiveDB};" -->
USE ${r"${hiveDB}"};

-- don't run joins locally, else risk running out of memory
SET hive.auto.convert.join=false;

-- setup for our custom, combinable deflated compression
-- See https://github.com/gbif/occurrence/issues/28#issuecomment-432958372
SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK;
SET mapred.output.compression.codec=org.gbif.hadoop.compress.d2.D2Codec;
SET io.compression.codecs=org.gbif.hadoop.compress.d2.D2Codec;
SET hive.merge.mapfiles=false;
SET hive.merge.mapredfiles=false;


-- in case this job is relaunched
DROP TABLE IF EXISTS ${r"${verbatimTable}"};
DROP TABLE IF EXISTS ${r"${interpretedTable}"};
DROP TABLE IF EXISTS ${r"${citationTable}"};
DROP TABLE IF EXISTS ${r"${multimediaTable}"};

-- pre-create verbatim table so it can be used in the multi-insert
CREATE TABLE ${r"${verbatimTable}"} (
<#list verbatimFields as field>
  ${field.hiveField} ${field.hiveDataType}<#if field_has_next>,</#if>
</#list>
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' TBLPROPERTIES ("serialization.null.format"="");

-- pre-create interpreted table so it can be used in the multi-insert
CREATE TABLE ${r"${interpretedTable}"} (
<#list interpretedFields as field>
  ${field.hiveField} ${field.hiveDataType}<#if field_has_next>,</#if>
</#list>
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' TBLPROPERTIES ("serialization.null.format"="");

<#if includeOccurrenceExtInterpreted>
  -- aux table to avoid clashes between column names in the joins since changing that in the code is pretty invasive
  CREATE TABLE ${r"${eventIdsTable}"} (event_id STRING);
</#if>

--
-- Uses multi-table inserts format to reduce to a single scan of the source table.
--
<#-- NOTE: Formatted below to generate nice output at expense of ugliness in this template -->
FROM iceberg.${r"${hiveDB}"}.${r"${tableName}"}
<#if isHumboldtSearch>
  LEFT JOIN iceberg.${r"${hiveDB}"}.${r"${tableName}"}_humboldt h ON h.gbifId = iceberg.${r"${hiveDB}"}.${r"${tableName}"}.gbifId
</#if>
  INSERT INTO TABLE ${r"${verbatimTable}"}
  SELECT
<#list verbatimFields as field>
    <#if field.hiveField == "gbifid">iceberg.${r"${hiveDB}"}.${r"${tableName}"}.</#if>${field.hiveField}<#if field_has_next>,</#if>
</#list>
  WHERE ${r"${whereClause}"}
  INSERT INTO TABLE ${r"${interpretedTable}"}
  SELECT
<#list initializedInterpretedFields as field>
    <#if field.hiveField == "gbifid">iceberg.${r"${hiveDB}"}.${r"${tableName}"}.</#if>${field.hiveField}<#if field_has_next>,</#if>
</#list>
  WHERE ${r"${whereClause}"}<#if !includeOccurrenceExtInterpreted>;</#if>
<#if includeOccurrenceExtInterpreted>
  INSERT INTO TABLE ${r"${eventIdsTable}"}
  SELECT DISTINCT eventid
  WHERE ${r"${whereClause}"};
</#if>


-- See https://github.com/gbif/occurrence/issues/28#issuecomment-432958372
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

--
-- Creates the multimedia table
-- Disabling hive auto join https://issues.apache.org/jira/browse/HIVE-2601.
SET hive.auto.convert.join=false;

CREATE TABLE ${r"${multimediaTable}"} ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' TBLPROPERTIES ("serialization.null.format"="")
AS SELECT m.gbifid, m.type, m.format, m.identifier, m.references, m.title, m.description, m.source, m.audience, m.created, m.creator, m.contributor, m.publisher, m.license, m.rightsHolder
FROM iceberg.${r"${hiveDB}"}.${r"${tableName}"}_multimedia m
JOIN ${r"${interpretedTable}"} i ON m.gbifId = i.gbifId;


<#if includeHumboldtInterpreted>
  -- Humboldt interpreted table
  CREATE TABLE ${r"${humboldtTable}"} (
  <#list humboldtFields as field>
    ${field.hiveField} ${field.hiveDataType}<#if field_has_next>,</#if>
  </#list>
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' TBLPROPERTIES ("serialization.null.format"="");

  INSERT INTO TABLE ${r"${humboldtTable}"}
  SELECT
  <#list humboldtSelectFields as field>
      <#if field.hiveField == "gbifid">h.</#if>${field.hiveField}<#if field_has_next>,</#if>
  </#list>
  FROM iceberg.${r"${hiveDB}"}.${r"${tableName}"}_humboldt h
  JOIN ${r"${interpretedTable}"} i ON h.gbifId = i.gbifId;
</#if>
<#if includeOccurrenceExtInterpreted>
   -- occurrence extension interpreted table
   CREATE TABLE ${r"${occurrenceExtensionTable}"} (
   <#list interpretedFields as field>
     ${field.hiveField} ${field.hiveDataType}<#if field_has_next>,</#if>
   </#list>
   ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' TBLPROPERTIES ("serialization.null.format"="");

   INSERT INTO TABLE ${r"${occurrenceExtensionTable}"}
   SELECT
   <#list initializedInterpretedFields as field>
     ${field.hiveField}<#if field_has_next>,</#if>
   </#list>
   FROM iceberg.${r"${hiveDB}"}.occurrence
   JOIN ${r"${eventIdsTable}"} ON eventid = event_id;
</#if>

SET hive.auto.convert.join=true;

--
-- Creates the citation table
-- At most this produces #datasets, so single reducer
-- creates the citations table, citation table is not compressed since it is read later from Java as TSV.
SET mapred.output.compress=false;
SET hive.exec.compress.output=false;

CREATE TABLE ${r"${citationTable}"}
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT datasetkey, count(*) as num_occurrences FROM ${r"${interpretedTable}"} WHERE datasetkey IS NOT NULL GROUP BY datasetkey;

