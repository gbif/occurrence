# GBIF Occurrence

The GBIF Occurrence project is a core component of the architecture responsible for everything to do with occurrence records - from initial parsing and interpretation through showing them on gbif.org, where they can be viewed in detail individually, and are searchable and downloadable. This project has many submodules and each of those has a README which you should read for more detail.

See also http://www.gbif.org/infrastructure/occurrences

## Building

Jenkins builds this project without a profile, and the produced artifacts (jars and wars) are used together with the corresponding configuration found in the gbif-configuration project. To run locally most modules require the dev profile to be activated, which itself can be found in http://github.com/gbif/gbif-configuration/maven/settings.xml.

e.g. mvn -Pdev clean install

## Testing

Run unit and integration tests:

```bash
mvn -Pdev clean verify
```

## Other documentation in this project

* [Occurrence persistence](occurrence-persistence/README.md)
* [Occurrence deleter](occurrence-deleter/README.md)
* [Occurrence WS](occurrence-ws/README.md)
* [Occurrence CLI](occurrence-cli/README.md)
* [Occurrence CLI – Previous Crawl Manager](occurrence-cli/doc/PreviousCrawlManager.md)
* [Occurrence download](occurrence-download/README.md)
* [Occurrence parser](occurrence-parser/README.md)
* [Occurrence common](occurrence-common/README.md)
* [Occurrence registry sync](occurrence-registry-sync/README.md)
* [Occurrence Hive](occurrence-hive/README.md)
* [Occurrence search](occurrence-search/README.md)
* [Occurrence processor](occurrence-processor/README.md)
* [Occurrence processor](occurrence-processor/doc/DefaultValues.md)
* [Occurrence index builder workflow](occurrence-index-builder-workflow/README.md)
* [Occurrence index builder workflow](occurrence-index-builder-workflow/bin/solr/README.md)
* [Occurrence Beam HBase SOLR](occurrence-beam-hbase-solr/README.md)
