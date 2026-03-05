# Occurrence Search WS

Elasticsearch-backed web service for **occurrence search**, **count**, **get-by-key**, and **metrics**. Deployed separately from [occurrence-ws](../occurrence-ws) so that ES load is isolated and the main occurrence API (HBase, downloads) can scale independently.

## What this service serves

- **Occurrence search** — `GET /occurrence/search` (query params), checklist-aware facets
- **Occurrence count** — `POST /occurrence/count` (body: predicate)
- **Occurrence by key** — `GET /occurrence/{gbifId}`, `GET /occurrence/{datasetKey}/{occurrenceId}`, and verbatim variants under `/occurrence/.../verbatim`
- **Metrics / cube** — count schema, cube reads, inventory endpoints (e.g. country, dataset indexes)

The gateway routes these paths to this service; occurrence-ws handles fragments, related occurrences, Annosys, and download request.

## Building

```bash
mvn clean package
```

## Configuration

Main settings (see [src/main/resources/application.yml](src/main/resources/application.yml)):

- **Elasticsearch:** `occurrence.search.es.hosts`, `occurrence.search.es.index`, timeouts
- **Search limits:** `occurrence.search.max.limit`, `occurrence.search.max.offset`
- **Species matching** (for checklist/scientific name): `nameUsageMatchingService.ws.url` or `api.url`
- **Checklist:** `defaultChecklistKey` (backbone when no checklistKey in request)
- **Cache (count/inventory):** `metrics-cache.expireAfterWrite`, `metrics-cache.entryCapacity`, `metrics-cache.refreshAhead`
- **API base:** `api.url` (e.g. for vocabulary/concept client)

## Running locally

Default port is 8081 (configurable via `server.port`).

```bash
cd occurrence-search-ws
mvn spring-boot:run
```


Then for example:

- Search: [http://localhost:8081/occurrence/search](http://localhost:8081/occurrence/search)
- Count: `curl -X POST -H "Content-Type: application/json" -d '{"type":"equals","key":"TAXON_KEY","value":"5242507"}' http://localhost:8081/occurrence/count`
- Get by key: [http://localhost:8081/occurrence/123456](http://localhost:8081/occurrence/123456)

## Dependencies

- **occurrence-search** — search and get-by-key implementation (ES)
- **occurrence-es-mapping** — ES mapping and request building
- **gbif-api**, **gbif-common-ws** — API models and path constants
