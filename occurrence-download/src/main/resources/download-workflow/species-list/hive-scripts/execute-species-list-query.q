USE ${hiveDB};

-- setup for our custom, combinable deflated compression
SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK;
SET mapred.output.compression.codec=org.gbif.hadoop.compress.d2.D2Codec;
SET io.compression.codecs=org.gbif.hadoop.compress.d2.D2Codec;

-- in case this job is relaunched
DROP TABLE IF EXISTS ${downloadTableName};
DROP TABLE IF EXISTS ${downloadTableName}_tmp;
DROP TABLE IF EXISTS ${downloadTableName}_citation;

-- pre-create verbatim table so it can be used in the multi-insert
CREATE TABLE ${downloadTableName}_tmp STORED AS ORC
AS SELECT taxonkey, scientificname, acceptedtaxonkey, acceptedscientificname, taxonrank, taxonomicstatus,
          kingdom, kingdomkey, phylum, phylumkey, class, classkey, order, orderkey, family, familykey,
          genus, genuskey, species, specieskey, iucnredlistcategory, datasetkey, license
FROM iceberg.${hiveDB}.occurrence
WHERE ${whereClause};


-- Creates the species tables, the use of COALESCE is to code defensively against possible null values
-- See https://github.com/gbif/occurrence/issues/28#issuecomment-432958372
SET hive.merge.mapfiles=false;
SET hive.merge.mapredfiles=false;
CREATE TABLE ${downloadTableName} ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("serialization.null.format"="")
AS SELECT taxonkey, scientificname, acceptedtaxonkey, acceptedscientificname, COUNT(taxonkey) AS numberOfOccurrences, taxonrank, taxonomicstatus, kingdom, kingdomkey,
          phylum, phylumkey, class, classkey, order, orderkey, family, familykey, genus, genuskey, species, specieskey, iucnredlistcategory
FROM ${downloadTableName}_tmp
GROUP BY taxonkey, scientificname, acceptedtaxonkey, acceptedscientificname, taxonrank, taxonomicstatus, kingdom, kingdomkey, phylum, phylumkey, class, classkey,
         order, orderkey, family, familykey, genus, genuskey, species, specieskey, iucnredlistcategory;

-- See https://github.com/gbif/occurrence/issues/28#issuecomment-432958372
SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-- creates the citations table, citation table is not compressed since it is read later from Java as TSV.
SET mapred.output.compress=false;
SET hive.exec.compress.output=false;
SET mapred.reduce.tasks=1;
CREATE TABLE ${downloadTableName}_citation ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT datasetkey, count(datasetkey) as citation, license
FROM ${downloadTableName}_tmp
WHERE datasetkey IS NOT NULL
GROUP BY datasetkey, license;

CREATE TABLE ${downloadTableName}_count AS SELECT count(*) FROM ${downloadTableName};
