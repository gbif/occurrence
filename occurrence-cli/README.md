# GBIF Occurrence CLI

This provides runnable services that subscribe to, and publish, occurrence events: processing, crawling, interpretation, creation, deletions and updates.

To run this build with maven and then add appropriate cluster configs (e.g. http://github.com/gbif/gbif-configuration/cli/dev/config) to the classpath when running individual services:
````mvn clean package````

Each service requires config files, both service config and logging config.

Example complete config files are given in the example-conf folder, with placeholders to supply the required values.

Examples (note you can pass a standard logback xml file in the properties as shown in the second example):

```bash
$ java -Xmx1G -cp /path/to/configs/:target/occurrence-cli-0.4-SNAPSHOT-jar-with-dependencies.jar update-occurrence-index --conf example-conf/indexing_run.yaml
$ java -Xmx1G -cp /path/to/configs/:target/occurrence-cli-0.4-SNAPSHOT-jar-with-dependencies.jar update-occurrence-index --conf example-conf/indexing_run.yaml --log-config indexing_logback.xml
```

NOTE: There are logging conflicts between xml Digester and this project (see http://dev.gbif.org/issues/browse/POR-2074) so your logback.xml should have the following line:

  <logger name="org.apache.commons.digester" level="ERROR"/>

If you run the application without any parameters, full instructions are given listing the available services.

It should be noted that you can override any property from the configuration file (or omit it) and supply it with the --property-name option.

## Commands available in this project:

Command | Description
--- | ---
delete-dataset | deletes an existing dataset
delete-occurrence | delete an occurrence record from HBase
fragment-processor | processes occurrence fragments
interpret-dataset | send a message to (re)interpret a dataset (starting from the interpreted-processor)
interpret-occurrence | send a message to (re)interpret a single occurrence (starting from the interpreted-processor)
interpreted-processor | create/updates the interpreted occurrence records
parse-dataset | send a message to (re)parse a dataset (starting from the verbatim-processor)
registry-change-listener | update the occurrence table when a dataset or organization changes country (via mapreduce)
verbatim-processor | create/updates the verbatim occurrence records
sync-occurrence-registry | Synchronize the entire Occurrence table with the registry (via mapreduce)

### Usage sync-occurrence-registry
This cli target will create a MapReduce job, wait for completion and terminate.
Example of how to resynchronize all occurrence records of a dataset with the registry:
```bash
java -Xmx128M -cp /path/to/config/:occurrence-cli.jar org.gbif.cli.Application sync-occurrence-registry --conf /path/to/config/sync-occurrence-registry.yaml --dataset-key <DATASETKEY> --log-config logback-sync-occurence-registry.xml
```
Note: The config folder must include a hbase-site.xml file

### Usage previous-crawls-manager
See [PreviousCrawlManager.md](https://github.com/gbif/occurrence/tree/master/occurrence-cli/doc/PreviousCrawlManager.md)

