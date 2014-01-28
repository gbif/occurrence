Occurrence Processor Service
--------------------------------------
This project listens for OccurrenceFragmentedMessages and then runs the entire processing chain, from
fragment->hbase->verbatim->hbase->interpretation->hbase. It sends messages as it progresses through the processing
chain, ending with OccurrencePersistedMessages.

Occurrence Interpreters
--------------------------------------
This is a library of interpretation/calculation/lookup routines for use in interpreting an Occurrence object from
a VerbatimOccurrence object.  Initially these are mostly just thin wrappers around existing processes. Note that
both CoordinateInterpreter and NubLookupInterpreter use live webservices.

Your maven profile needs to specify the following properties (shown with typical example services):

<occurrence.registry.ws.url>http://crawler.gbif.org:8080/registry-ws/</occurrence.registry.ws.url>
<occurrence.nub.ws.url>http://apidev.gbif.org/lookup/name_usage/</occurrence.nub.ws.url>
<occurrence.geo.ws.url>http://apidev.gbif.org/lookup/reverse_geocode/</occurrence.geo.ws.url>
