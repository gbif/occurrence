# GBIF Occurrence

The GBIF Occurrence project is a component of the architecture responsible for everything to do with occurrence records.

Initial parsing and interpretation is handled by the [Pipelines](https://github.com/gbif/pipelines/) project.  This project handles occurrence
web services, downloads, search and maps.

This project has many submodules and each of those has a README which you should read for more detail.

## Building

Jenkins builds this project without a profile, and the produced artifacts (JARs) are used together with the corresponding configuration found in the gbif-configuration project. To run locally most modules require the dev profile to be activated, which itself can be found in https://github.com/gbif/gbif-configuration/maven/settings.xml.

e.g. `mvn -Pdev clean install`

## Contributing
* All changes must go to the **dev** branch for testing before merging to master.
* PR are preferred for complex functionality. **Please target the dev branch**.
* Simple changes can be committed without review.


## Testing

Run unit and integration tests:

```bash
mvn -Pdev clean verify
```

## Other documentation in this project

* [Occurrence persistence](occurrence-persistence/README.md)
* [Occurrence WS](occurrence-ws/README.md)
* [Occurrence CLI](occurrence-cli/README.md)
* [Occurrence download](occurrence-download/README.md)
* [Occurrence common](occurrence-common/README.md)
* [Occurrence registry sync](occurrence-registry-sync/README.md)
* [Occurrence Hive](occurrence-hive/README.md)
* [Occurrence search](occurrence-search/README.md)
* [Occurrence heatmaps](occurrence-heatmaps/README.md)
* [Occurrence processor](occurrence-processor/README.md)
* [Occurrence processor (Default values)](occurrence-processor/doc/DefaultValues.md)
