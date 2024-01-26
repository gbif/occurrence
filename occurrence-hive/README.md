# GBIF Occurrence Hive

A separate module to hold the Hive User Defined Functions (UDFs) currently only used by the analytics processing.  Also useful when testing changes to parsers.

# GBIF Occurrence Processor Service

This project used to listen for OccurrenceFragmentedMessages and then run the entire processing chain.

That functionality has been replaced by pipelines, but a small part of the processing is still used in the Hive UDFs,
and therefore for the analytics processing.

## Occurrence Interpreters

This is a library of interpretation/calculation/lookup routines for use in interpreting an Occurrence object from
a VerbatimOccurrence object.  Initially these are mostly just thin wrappers around existing processes. Note that
both CoordinateInterpreter and NubLookupInterpreter use live webservices.

````mvn -Pdev clean install````
