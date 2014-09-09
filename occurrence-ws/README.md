occurrence-ws
======================

# Brief information about the featured occurrences (dot's on homepage):

Create an hbase table which is named by the propery in the pom and should be populated with something like this:

```
SELECT cell, collect_set(occId)
FROM
(
  SELECT max(id) as occId, dataset_id, owning_org_id, concat(pmod(cast(latitude AS int),5), ':', pmod(cast(longitude AS int),5)) as cell
  FROM uat_occurrence_hdfs
  WHERE
    dataset_id IS NOT NULL AND
    owning_org_id IS NOT NULL AND
    latitude>=-85 AND latitude<=85 AND
    longitude>=-180 AND longitude<=180 AND
    geospatial_issue=0 AND
    kingdom_id IN(1,2,3,4,5,6,7,8)
  GROUP BY
    dataset_id,
    owning_org_id,
    kingdom_id,
    pmod(cast(latitude AS int),5),
    pmod(cast(longitude AS int),5)
) t1
GROUP BY cell;
```


## Downloads
This is the webservice that orchestrates occurrence downloads by accepting a json download string, translating it
into a Hive download, kicking off the Oozie workflow that calls Hive, and then responding to the original request
with the location of the download.


Testing it
----------

An example json file as input to the service is included in src/test/resources and it can be posted to this
webservice using curl as below:

```
curl -X POST --header "Content-Type:application/json" --user username:password -d @sample_taxon_key.json http://localhost:8080/occurrence/download/request
```
