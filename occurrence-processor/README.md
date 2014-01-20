Occurrence Processor Service
--------------------------------------
This project listens for OccurrenceFragmentedMessages and then runs the entire processing chain, from
fragment->hbase->verbatim->hbase->interpretation->hbase. It sends messages as it progresses through the processing
chain, ending with OccurrencePersistedMessages.