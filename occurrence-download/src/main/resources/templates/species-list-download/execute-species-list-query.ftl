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

-- in case this job is relaunched
DROP TABLE IF EXISTS ${r"${speciesListTable}"};
DROP TABLE IF EXISTS ${r"${speciesListTable}"}_tmp;
DROP TABLE IF EXISTS ${r"${speciesListTable}"}_citation;

-- pre-create verbatim table so it can be used in the multi-insert
CREATE TABLE ${r"${speciesListTable}"}_tmp ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT
taxonkey , scientificname, taxonrank, taxonomicstatus, kingdom, kingdomkey, phylum, phylumkey,class,classkey, order_, orderkey, family,familykey, genus,genuskey, subgenus, subgenuskey, species, specieskey , datasetkey
FROM occurrence_hdfs
WHERE ${r"${whereClause}"};

CREATE TABLE ${r"${speciesListTable}"} ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("serialization.null.format"="")
AS SELECT
taxonkey , scientificname, count(taxonkey) as no_of_occurrences, taxonrank, taxonomicstatus, kingdom, kingdomkey, phylum, phylumkey,class,classkey, order_, orderkey, family,familykey, genus,genuskey, subgenus, subgenuskey, species, specieskey
FROM ${r"${speciesListTable}"}_tmp
GROUP BY 
taxonkey, scientificname, taxonrank, taxonomicstatus, kingdom, kingdomkey,phylum, phylumkey, class,classkey,order_, orderkey, family, familykey, genus, genuskey , subgenus, subgenuskey, species, specieskey;

-- creates the citations table, citation table is not compressed since it is read later from Java as TSV.
SET mapred.output.compress=false;
SET hive.exec.compress.output=false;

CREATE TABLE ${r"${speciesListTable}"}_citation ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT
datasetkey, count(datasetkey) as citation
FROM ${r"${speciesListTable}"}_tmp WHERE datasetkey IS NOT NULL GROUP BY datasetkey;

CREATE TABLE ${r"${speciesListTable}"}_count AS SELECT count(*) FROM ${r"${speciesListTable}"};
