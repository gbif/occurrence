# GBIF Occurrence Download

This project provides the Oozie workflows used to:

1. Create a set of Hive tables used by occurrence download: occurrence_pipeline_hdfs, occurrence_pipeline_avro and occurrence_pipeline_multimedia.
2. Occurrence Download through GBIF.org using the occurrence-ws

## Installation

### Oozie coordinator installation

This coordinator periodically builds the Hive tables used for Occurrence downloads. 

It kills the current coordinator job, copies and installs the new job using a Maven custom build that generates Hive scripts based on table definitions.

To install the Oozie coordinator use the script [install-workflow.sh](install-workflow.sh) which supports the following parameters:

1. Environment (required): and existing profile in the Maven [file](https://github.com/gbif/gbif-configuration/blob/master/occurrence-download/profiles.xml) in the gbif-configuration repository.
2. GitHub authentication token (required): authentication token to access the [gbif-configuration](https://github.com/gbif/gbif-configuration/) repository.
3. Source directory (default to hdfs://ha-nn/data/hdfsview/occurrence/): directory to stores the input Avro files.
4. Occurrence base table name (default to occurence_pipeline): common table name to be used for the Avro, Hdfs and Multimedia tables.

The Oozie coordinator job will be installed in the HDFS directory __occurrence-download-workflows__*environmentName*.

In general terms the workflow follows the following steps:

1. Take an HDFS [snapshot](https://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-hdfs/HdfsSnapshots.html) of the source directory. The snapshot name will have the Oozie workflow ID.
2. Run a set of Hive statements to: build an Avro external table that points to the source directory, creates the HDFS table that contains all the occurrence records and builds the table with associated media.
3. Deletes the HDFS snapshot. 


### Schema changes

If the current Occurrence project version imposes a new schema for the Hive tables the script [run-workflow-schema-migration.sh](run-workflow-schema-migration.sh).

The Oozie workflow executed is the same used by the coordinator job described above, with the only difference that the *schema_change* flag is enabled.

By activating the flag the produced tables will have the prefix *new_* as part of their names. 

Existing tables are renamed with the prefix *old_* and *new_* tables are renamed to use expected names by the download workflows.

The Oozie coordinator job will be installed in the HDFS directory __occurrence-download-workflows-new-schema-__*environmentName*.

After running this script and workflow successfully, the download workflows must be updated using the [install-workflow.sh](install-workflow.sh) script.

