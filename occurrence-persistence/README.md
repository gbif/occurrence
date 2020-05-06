# GBIF Occurrence Persistence Service

The main occurrence persistence service has been migrated to Pipelines.

This project provides read-only access to occurrence fragments. To use it you need to specify the following properties and make them available to Guice:

occurrence.db.fragmenter_name
occurrence.db.max_connection_pool
occurrence.db.zookeeper.connection_string

This is usually done by adding these properties to a configuration file which is added to the classpath of the wrapping application.
For local development you can also use the dev profile from the `gbif-configuration` project in `maven/settings.xml` as follows:

```
mvn -Pdev clean install
```
