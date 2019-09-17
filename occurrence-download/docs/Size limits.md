Occurrence download size limits
===============================

These are the relevant steps for an occurrence download with a long and/or complex query.

→ User tests query using GET requests, e.g. through the portal.  This required [this change](https://github.com/gbif/gbif-microservice/commit/a3098c38a78050d3233671c3a8e31f918f335823)
  to allow configuration of the maximum request length.

→ User POSTs request to API endpoint
  → Possible limit on POST data length
    → An error is returned here, if the query is probably too long or complex
  → Filter is converted into Java objects
  → Job is POSTed to Oozie
    → Oozie serializes the DownloadPrepareAction as Java Properties.  The size is limited by the Oozie configuration parameter
      `oozie.action.max.output.data`.
  → The occurrence download is POSTed to the registry
    → The registry saves it in PostgreSQL

  → Oozie runs the job.  If the SOLR query (GET) is too long, the download will run on Hive even if it's small.
    SOLR has the configuration option `-Dsolr.jetty.request.header.size=1048576` set.
    The test for a small download queries SOLR, this can fail in DownloadPrepareAction in these ways
    • "too many boolean clauses" for a long list of taxa.  I can't reproduce this using curl, as I get the too-large errors instead.
      It should be safe though (either above the limit or not).
    • URI too long.
  → Oozie updates the job status, using a GET to occurrence-ws (just the status, there's no problem with the predicate size here)
    → This sends a PUT to registry-ws
      → If the download status becomes `SUCCESSFUL`, the Registry updates the DOI.  This serializes the query into readable form,
        which includes looking up dataset tiles and taxon names.  download-query-tools won't convert predicates with >10500 parameters,
        to avoid the many lookups that would be required.
        → The DOI updater synchronizes the DOI with DataCite.
      → The download is completed.

The target is to allow a download of 100,000 taxon keys, since this covers most custom downloads based on a checklist.  The
serialized DownloadPrepareAction requires a little over 10MB for this, so it is set to 20MiB:

| Oozie configuration            | value      |
|--------------------------------|------------|
| `oozie.action.max.output.data` | `20971520` |

Polygon queries are slow if there are many points, therefore limit these to 5,000 points.

(A possible way to increase the polygon size limit would be with a bounding box, or simplified (but enveloping) polygon.  This isn't
implemented.)
