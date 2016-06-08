# GBIF Occurrence Web Services
These web services provide two important parts of gbif.org: the occurrence details themselves, and the download service. It is typically
deployed once, but then accessed through two different varnish backends - one for occurrence details and one for downloads (in order to
isolate the often slow download threads from the fast occurrence detail lookups).

## Building
To produce the artifact that will be deployed just run `mvn clean package`. This will strip all local configs and expects that you
provide configs that match your destination environment (i.e. dev, uat, or prod - get the configs from gbif-configuration).

## Usage
For local development and testing you can run this project using `mvn -Pdev jetty:run`. You will need to configure a local dev profile like the following, and you need to provide a maven profile that contains these two parameters:
  1. occurrence.download.ws.password
  2. drupal.db.password

The dev profile from http://github.com/gbif/gbif-configuration/maven/settings.xml should do the job.
  
Then test it with `http://localhost:8080/occurrence/12345`

Note the empty .password properties you need to fill in:

````xml
    <profile>
      <id>dev</id>
      <properties>
        <!-- cluster -->
        <zookeeper.quorum>c1n1.gbif.org:2181,c1n2.gbif.org:2181,c1n3.gbif.org:2181</zookeeper.quorum>
        <hadoop.jobtracker>c1n2.gbif.org:8032</hadoop.jobtracker>
        <hdfs.namenode>hdfs://c1n1.gbif.org:8020</hdfs.namenode>
        <oozie.url>http://c1n1.gbif.org:11000/oozie</oozie.url>
        <hive.metastore.uris>thrift://c1n1.gbif.org:9083</hive.metastore.uris>

        <!-- determines how often the HDFS tables are built -->
        <occurrence.hdfs.build.frequency>${coord:hours(4)}</occurrence.hdfs.build.frequency>

        <!-- webservices -->
        <occurrence.search.solr.server>http://apps2.gbif-dev.org:8081/occurrence-solr/</occurrence.search.solr.server>
        <occurrence.download.ws.url>http://api.gbif-dev.org/v1/</occurrence.download.ws.url>
        <occurrence.download.hive.hdfs.out>/user/hive/warehouse/dev.db</occurrence.download.hive.hdfs.out>
        <occurrence.download.ws.username>occdownload.gbif.org</occurrence.download.ws.username>
        <occurrence.download.ws.password></occurrence.download.ws.password>
        <occurrence.download.zookeeper.sleep_time>1000</occurrence.download.zookeeper.sleep_time>
        <occurrence.download.zookeeper.max_retries>10</occurrence.download.zookeeper.max_retries>
        <occurrence.download.zookeeper.lock_name>occDownloadJobsCounter</occurrence.download.zookeeper.lock_name>
        <occurrence.download.max_global_threads>10</occurrence.download.max_global_threads>
        <occurrence.download.job.max_threads>3</occurrence.download.job.max_threads>
        <occurrence.download.job.min_records>200</occurrence.download.job.min_records>
        <occurrence.download.file.max_records>1000</occurrence.download.file.max_records>
        <occurrence.download.datause.url>http://www.gbif-dev.org/faq/datause</occurrence.download.datause.url>
        <checklistbank.match.ws.url>http://api.gbif-dev.org/v1/species/match/</checklistbank.match.ws.url>
        <checklistbank.ws.url>http://api.gbif-dev.org/v1/</checklistbank.ws.url>
        <registry.ws.url>http://api.gbif-dev.org/v1/</registry.ws.url>
        <geocode.ws.url>http://api.gbif.org/v1/geocode/reverse/</geocode.ws.url>
        <api.url>http://api.gbif-dev.org/v1/</api.url>
        <!-- security -->
        <drupal.db.host>my1.gbif-dev.org</drupal.db.host>
        <drupal.db.url>jdbc:mysql://my1.gbif-dev.org:3306/drupal_devel?useUnicode=true&amp;characterEncoding=UTF8&amp;characterSetResults=UTF8</drupal.db.url>
        <drupal.db.name>drupal_dev</drupal.db.name>
        <drupal.db.username>drupal_dev</drupal.db.username>
        <drupal.db.password></drupal.db.password>
        <drupal.db.poolSize>8</drupal.db.poolSize>
        <drupal.db.connectionTimeout>2000</drupal.db.connectionTimeout>

        <appkeys.file>/usr/local/gbif/conf/appkeys.properties</appkeys.file>
      </properties>
    </profile>
````









### Brief information about the featured occurrences (dot's on homepage):

Create an hbase table which is named by the property in the pom and should be populated with something like this:

```
INSERT OVERWRITE TABLE featured_occurrence 
SELECT cell, collect_set(gbifid)
FROM
(
  SELECT max(gbifid) as gbifid, datasetkey, publishingorgkey, concat(pmod(cast(decimallatitude AS int),5), ':', pmod(cast(decimallongitude AS int),5)) as cell
  FROM occurrence_hdfs
  WHERE
    datasetkey IS NOT NULL AND
    publishingorgkey IS NOT NULL AND
    decimallatitude>=-85 AND decimallatitude<=85 AND
    decimallongitude>=-180 AND decimallongitude<=180 AND
    hasgeospatialissues=FALSE AND
    kingdomkey IN(1,2,3,4,5,6,7,8)
  GROUP BY
    datasetkey,
    publishingorgkey,
    kingdomkey,
    pmod(cast(decimallatitude AS int),5),
    pmod(cast(decimallongitude AS int),5)
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
