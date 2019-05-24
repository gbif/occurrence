# Salted key tests

Tests that 2 distinct processes assigning keys using the same lookup table can coexist.
Tests use the Rabbit/Solr ingestion and the Beam/ES pipeline.

**Summary: The results confirm things operate as expected**

## Tests

4 datasets were used with counts:
  - 1534
  - 170,040
  - 192,863
  - 290,333

# Test 1

Purpose: establish baseline counts and lookups with tried-and-tested Rabbit/Solr.

Rabbit/Solr only running, 4 datasets
```
654,770 SOLR
1,115,142 Lookup
```

HBase tables snapshoted:
- tim_test1_counter
- tim_test1_lookup

## Test 2:

Purpose: ensure Beam/ES matches baseline when running in isolation.

Truncate all HBase tables from `test 1` and then run the pipeline only by stopping the `occurrence-processors`.

Pipelines only running, 4 datasets
```
654,770 ES
1,115,142 Lookup
```

HBase tables snapshoted:
- tim_test2_counter
- tim_test2_lookup


## Test 3:

Purpose: Ensure lookups created by pipelines are used by the Rabbit/Solr process.

The final state of `Test 2` left messages on the Rabbit queues. By starting the `occurrence-processors` we can consume these knowing the IDs are already assigned.

Rabbit only processing, but both running, 4 datasets
```
654,770 SOLR
1,115,142 Lookup
All records show as new (500-1000/s)
```


## Test 4:

Purpose: Ensure that when keys are assinged both processes can read them without minting new IDs.

Issue a crawl for everything.

```
654,770 SOLR
654,770 ES
1,115,142 Lookup
All records show as unchanged - all ran through fast
```


## Test 5:

Purpose: Verify that high concurrency ID assignment from parallel processes results in the same state as when run in isolation.

All tables, SOLR, ES truncated.

```
654,770 SOLR
654,770 ES
1,115,142 Lookup
All records show as unchanged - all ran through fast
```

HBase tables snapshoted:
- tim_test3_counter
- tim_test3_lookup

## Test 6:

Purpose: Similar to `Test 5` but runs for a sustained period to ensure robustness.

Index iNatarulist concurrently on top of `test 5`. Concurrent ID assigning will run for a long duration.

```
9,053,152 SOLR
9,053,152 ES
9,513,524 Lookup (= Test 4 + dataset size)
All records show as unchanged - all ran through fast
```

A script was used to pull all IDs from Solr and ES and confirm the same IDs were in both.

HBase tables snapshoted:
- tim_test4_counter
- tim_test4_lookup


# Scripts

## Initiate crawls

On `devcrawler1-vh.gbif.org` crawls can be initiated (forcing update to override `conditional GET`
```
cd /home/crap/util
./crawl-cleanup --clean-cache --recrawl 8575f23e-f762-11e1-a439-00145eb45e9a
./crawl-cleanup --clean-cache --recrawl 76dd8f0d-2daa-4a69-9fcd-55e04230334a
./crawl-cleanup --clean-cache --recrawl 96ca66b4-f762-11e1-a439-00145eb45e9a
./crawl-cleanup --clean-cache --recrawl 33948acc-bfac-4107-92c8-6464de387822
```

## Recreating HBase

The following created all new HBase tables
```
disable 'dev_occurrence'
drop 'dev_occurrence'
disable 'dev_occurrence_counter'
drop 'dev_occurrence_counter'
disable 'dev_occurrence_lookup'
drop 'dev_occurrence_lookup'

create 'dev_occurrence', {NAME => 'o', BLOOMFILTER => 'ROW', COMPRESSION => 'SNAPPY' }
create 'dev_occurrence_counter', {NAME => 'o', BLOOMFILTER => 'ROW', COMPRESSION => 'SNAPPY'}
create 'dev_occurrence_lookup',
  {NAME => 'o', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'ROW'},
{SPLITS => [
    '01','02','03','04','05','06','07','08','09','10',
    '11','12','13','14','15','16','17','18','19','20',
    '21','22','23','24','25','26','27','28','29','30',
    '31','32','33','34','35','36','37','38','39','40',
    '41','42','43','44','45','46','47','48','49','50',
    '51','52','53','54','55','56','57','58','59','60',
    '61','62','63','64','65','66','67','68','69','70',
    '71','72','73','74','75','76','77','78','79','80',
    '81','82','83','84','85','86','87','88','89','90',
    '91','92','93','94','95','96','97','98','99'
  ]}

disable 'dev_occurrence_cube'
drop 'dev_occurrence_cube'
disable 'dev_dataset_country_cube'
drop 'dev_dataset_country_cube'
disable 'dev_dataset_taxon_cube'
drop 'dev_dataset_taxon_cube'

create 'dev_occurrence_cube', {NAME => 'dc', BLOOMFILTER => 'ROW', COMPRESSION => 'NONE'}
create 'dev_dataset_country_cube', {NAME => 'dc', BLOOMFILTER => 'ROW', COMPRESSION => 'NONE'}
create 'dev_dataset_taxon_cube', {NAME => 'dc', BLOOMFILTER => 'ROW', COMPRESSION => 'NONE'}

```

## Truncate search indexes

Truncate Solr:

```
curl http://c3n1.gbif.org:8983/solr/occurrence/update -H "Content-type: text/xml" --data-binary '<delete><query>*:*</query></delete>'
curl http://c3n1.gbif.org:8983/solr/occurrence/update -H "Content-type: text/xml" --data-binary '<commit />'
```

Truncate ES (run through Postman, but this should work):
```
curl http://c3n1.gbif.org:9200/occurrence/_delete_by_query -H "Content-type: application/json" --data-binary '{"query": {"match_all": {}}}'
```

Truncate varnish
```
curl -Ssi --request FLUSH http://api-solr.gbif-dev.org/
curl -Ssi --request FLUSH http://www-solr.gbif-dev.org/

curl -Ssi --request FLUSH http://api-es.gbif-dev.org/
curl -Ssi --request FLUSH http://www-es.gbif-dev.org/
```
