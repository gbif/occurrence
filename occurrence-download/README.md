# GBIF Occurrence Download

This project provides the Oozie workflows used to:

1. Create a set of Hive tables used by occurrence download: occurrence, occurrence_avro and occurrence_multimedia.
2. Occurrence Download through GBIF.org using the occurrence-ws

## Installation

### Oozie coordinator installation

This coordinator periodically builds the Hive tables used for Occurrence downloads.

It kills the current coordinator job, copies and installs the new job using a Maven custom build that generates Hive scripts based on table definitions.

To install the Oozie coordinator use the script [install-workflow.sh](install-workflow.sh) which supports the following parameters:

1. Environment (required): and existing profile in the Maven [file](https://github.com/gbif/gbif-configuration/blob/master/occurrence-download/profiles.xml) in the gbif-configuration repository.
2. GitHub authentication token (required): authentication token to access the [gbif-configuration](https://github.com/gbif/gbif-configuration/) repository.
3. Source directory (default to hdfs://ha-nn/data/hdfsview/occurrence/): directory to stores the input Avro files.
4. Occurrence base table name (default to occurence): common table name to be used for the Avro, Hdfs and Multimedia tables.

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

### Applying schema changes manually
Alternatively to the process described above, the tables can be created and swap manually by following these steps:

1. Build the occurrence-download project, that will create a directory `occurrence-download-workflows-dev` in the Maven target directory.

2. Copy the file `target/occurrence-download-workflows-dev/create-tables/avro-schemas\occurrence-hdfs-record.avsc` to a known location in HDFS.

3. From a host with access to the Hive CLI:
  - Download the occurrence-hive and occurrence-download from Nexus, for example:
    `curl https://repository.gbif.org/repository/releases/org/gbif/occurrence/occurrence-download/0.150/occurrence-download-0.150.jar -o occurrence-download.jar`
    `curl https://repository.gbif.org/repository/releases/org/gbif/occurrence/occurrence-download/0.150/occurrence-hive-0.150.jar -o occurrence-hive.jar`

4. Run the Hive CLI: `sudo -u hdfs hive`

5. Add the downloaded JAR files:
   `ADD JAR occurrence-hive.jar;`
   `ADD JAR occurrence-download.jar;`

6. In the file `target/occurrence-download-workflows-dev/create-tables/hive-scriptscreate-occurrence-avro.q`

   - Create an Avro table using the schema file copied in the step above and change the path `hdfsPath` to that location, change  `hdsDataLocation` to the path were the data is located, usually `/data/hdfsview/occurrence` :
   ```
   CREATE EXTERNAL TABLE occurrence_avro_new
   ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
   STORED as INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
   OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
   LOCATION 'hdsDataLocation'
   TBLPROPERTIES ('avro.schema.url'='hdfsPath/occurrence-hdfs-record.avsc');
   ```
   - Replace `${downloadTableName}` with `occurrence_new`, `${downloadTableName}_avro` with `occurrence_avro_new`, `${downloadTableName}_multimedia` with `occurrence_multimedia_new`
   - Run every statement of the script `hive-scriptscreate-occurrence-avro.q`.
7. Test the new tables and, if needed, rename the Hive tables:
```
ALTER TABLE occurrence RENAME TO occurrence_old;
ALTER TABLE occurrence_multimedia RENAME TO occurrence_multimedia_old;
ALTER TABLE occurrence_avro RENAME TO occurrence_avro_old;

ALTER TABLE occurrence_new RENAME TO occurrence;
ALTER TABLE occurrence_multimedia_new RENAME TO occurrence_multimedia;
ALTER TABLE occurrence_avro_new RENAME TO occurrence_avro;
```
8. Install the new workflow by running the Jenkins designated for that, that job runs the `install-workflow.sh` job and accepts a git branch or tag as parameter.

