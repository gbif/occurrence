<#--
  This is a freemarker template which will generate an HQL script which is run at download time.
  When run in Hive as a parameterized query, this will create a set of tables ...
  TODO: document when we actually know something accurate to write here...
-->
<#-- Required syntax to escape Hive parameters. Outputs "USE ${hiveDB};" -->
USE ${r"${hiveDB}"};

-- setup for our custom, combinable deflated compression
SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK;
SET mapred.output.compression.codec=org.gbif.hadoop.compress.d2.D2Codec;
SET io.compression.codecs=org.gbif.hadoop.compress.d2.D2Codec;
CREATE TEMPORARY FUNCTION toISO8601 AS 'org.gbif.occurrence.hive.udf.ToISO8601UDF';
CREATE TEMPORARY FUNCTION contains AS 'org.gbif.occurrence.hive.udf.ContainsUDF';
CREATE TEMPORARY FUNCTION joinArray AS 'brickhouse.udf.collect.JoinArrayUDF';

-- in case this job is relaunched
DROP TABLE IF EXISTS ${r"${occurrenceTable}"};

-- pre-create verbatim table so it can be used in the multi-insert
CREATE TABLE ${r"${occurrenceTable}"} ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("serialization.null.format"="")
AS SELECT
taxonkey , scientificname, vernacularname, count(taxonkey) no_of_occurrences, taxonrank, taxonomicstatus, kingdom, kingdomkey, phylum, phylumkey,class,classkey, order_, orderkey, family,familykey, genus,genuskey, subgenus, subgenuskey, species, specieskey
FROM occurrence_hdfs
WHERE ${r"${whereClause}"}
GROUP BY 
taxonkey, scientificname, taxonrank, taxonomicstatus, kingdom, kingdomkey,phylum, phylumkey, class,classkey,order_, orderkey, family, familykey, genus, genuskey , subgenus, subgenuskey, species, specieskey, vernacularname;

