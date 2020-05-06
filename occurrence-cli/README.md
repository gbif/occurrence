# GBIF Occurrence CLI

This provides runnable services that subscribe to occurrence events.

After the migration of most functionality to Pipelines, only two remain:

* EsDatasetDeleter: deletes indices from ElasticSearch.  This should probably be moved to the Pipelines project.
* RegistryChange: tracks changes to the registry and starts new crawls if necessary.  This should be moved to the Registry project.

To run this build with maven and then add appropriate cluster configs (e.g. https://github.com/gbif/gbif-configuration/cli/dev/config) to the classpath when running individual services:
````mvn clean package````

Each service requires config files, both service config and logging config.

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
registry-change-listener | request a new crawl when a dataset or organization changes country or publisher.
