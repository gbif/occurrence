The occurrence CLI project
=======================

This provides runnable services that subscribe and publish  occurrences events: processing, crawling, interpretation, creation, deletions and updates..


To run this, it is expected that you would build the project as a single jar (environment specific for dev, uat, or prod_a):
  $ mvn -Pdev clean package assembly:single

And then run it using a config file.

Example complete config files are given in the example-conf folder, with placeholders to supply the required values.

Examples (note you can pass a standard logback xml file in the properties as shown in the second example):

$ java -Xmx1G -jar target/occurrence-cli-0.4-SNAPSHOT-jar-with-dependencies.jar update-occurrence-index --conf example-conf/indexing_run.yaml
$ java -Xmx1G -jar target/occurrence-cli-0.4-SNAPSHOT-jar-with-dependencies.jar update-occurrence-index --conf example-conf/indexing_run.yaml --log-config indexing_logback.xml

NOTE: There are logging conflicts between xml Digester and this project (see http://dev.gbif.org/issues/browse/POR-2074) so your logback.xml should have the following line:

  <logger name="org.apache.commons.digester" level="ERROR"/>

If you run the application without any parameters, full instructions are given listing the available services.

It should be noted that you can override any property from the configuration file (or omit it) and supply it with the --property-name option.

Commands available in this project:
====================================

delete-occurrence-index: deletes occurrence records from the Solr Index
update-occurrence-index: inserts/updates occurrence records into the Solr Index
fragment-processor: processes occurrence fragments
interpreted-processor: create/updates the interpreted occurrence records
verbatim-processor: create/updates the verbatim occurrence records
delete-occurrence: delete an occurrence record from HBase
delete-data-resource: deletes a a data resource
delete-dataset: deletes an existing dataset
