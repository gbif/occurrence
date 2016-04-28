# GBIF Occurrence Persistence Service

This project is a library that serves as an implementation of the Occurrence Service that reads and writes occurrence objects from HBase. In order
to use it you need to specify the following properties and make them available to Guice:

occurrence.db.table_name
occurrence.db.counter_table_name
occurrence.db.id_lookup_table_name
occurrence.db.max_connection_pool
occurrence.db.zookeeper.connection_string

This is usually done by adding these properties to a configuration file which is added to the classpath of the wrapping application.
For local development you can also use the dev profile from the `gbif-configuration` project in `maven/settings.xml` as follows:

```
mvn -Pdev clean install
```

## Tests

To run the tests, a mini-cluster will be created automatically on the filesystem. No configuration is required.
```bash
mvn clean test
```