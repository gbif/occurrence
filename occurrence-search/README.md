# GBIF Occurrence Search

## Records from HBase

Elasticsearch can be used only to search: when an HBase records table is configured, the search
requests don't fetch the `_source` of the documents, and the records of the returned ids are read
from the table ([gbif/pipelines#1534](https://github.com/gbif/pipelines/issues/1534), see
`docs/hbase-records-tables.md` in pipelines for the layout of the tables).

| Service             | Setting                                           | Table               |
|---------------------|---------------------------------------------------|---------------------|
| occurrence-ws       | `occurrence.search.records.table`                 | occurrence records  |
| event-ws            | `occurrence.search.records.table`                 | event records       |
| small downloads     | `records.table` (plus any `hbase.*` HBase setting) | occurrence records  |

When the setting is empty or missing, the records are built from the Elasticsearch `_source`, as
before. The HBase connection uses the `hbase-site.xml` of the classpath.

- `occurrence/{key}` and `occurrence/{key}/verbatim` (`event/{key}` in event-ws) are read from
  HBase directly, without querying Elasticsearch.
- Searches query Elasticsearch for the ids of the page and read the records with one multi-get,
  keeping the order of the hits and skipping the records that no longer exist.
- Small downloads read both views of the records: the interpreted record with the complete verbatim
  record, which includes the verbatim terms the API leaves out of the interpreted view.
