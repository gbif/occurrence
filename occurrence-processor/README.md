# GBIF Occurrence Processor Service

This project listens for OccurrenceFragmentedMessages and then runs the entire processing chain, from
fragment->hbase->verbatim->hbase->interpretation->hbase. It sends messages as it progresses through the processing
chain, ending with OccurrencePersistedMessages.

## Occurrence Interpreters

This is a library of interpretation/calculation/lookup routines for use in interpreting an Occurrence object from
a VerbatimOccurrence object.  Initially these are mostly just thin wrappers around existing processes. Note that
both CoordinateInterpreter and NubLookupInterpreter use live webservices.

This library in turn uses the Occurrence Persistence library, and so needs all the configs to talk to HBase and the GBIF
API. It is currently only used by the Occurrence CLI module during crawling. For local testing use the dev profile
from http://github.com/gbif/gbif-configuration/maven/settings.xml and build as:

````mvn -Pdev clean install````
