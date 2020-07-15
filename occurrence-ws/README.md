# GBIF Occurrence Web Services

These web services provide two important parts of GBIF.org: the occurrence details themselves, and the download service. It is typically
deployed once, but then accessed through two different Varnish backends – one for occurrence details and one for downloads (in order to
isolate the often slow download threads from the fast occurrence detail lookups).

## Building
To produce the artifact that will be deployed just run `mvn clean package`. This will strip all local configs and expects that you
provide configs that match your destination environment (i.e. dev, uat, or prod – get the configs from gbif-configuration).

## Usage
To run this project from your machine you have to:
 1. Configure profile like the following than contains the following entries:

```
        api.url
        registry.ws.url
        checklistbank.match.ws.url
        checklistbank.ws.url
        occurrence.env_prefix
        occurrence.environment

        registry.db.url
        registry.db.password
        registry.db.username
        registry.db.hikari.idleTimeout
        registry.db.hikari.maximumPoolSize
        registry.db.hikari.minimumIdle

        occurrence.db.fragmenterTable
        occurrence.db.fragmenterSalt
        occurrence.db.hbasePoolSize
        occurrence.db.zkConnectionString


        occurrence.search.es.hosts
        occurrence.search.es.index
        occurrence.search.es.connect_timeout
        occurrence.search.es.socket_timeout
        occurrence.search.max.offset
        occurrence.search.max.limit

        occurrence.download.ws.username
        occurrence.download.ws.password
        occurrence.download.ws.url
        occurrence.download.portal.url
        occurrence.download.ws.mount
        occurrence.download.oozie.url
        occurrence.download.hive.db
        occurrence.download.hdfs.namenode
        occurrence.download.environment
        occurrence.download.hive.hdfs.out
        occurrence.download.user.name
        occurrence.download.max_user_downloads

        occurrence.download.downloads_max_points

        occurrence.download.downloads_max_predicates
        occurrence.download.downloads_soft_limit
        occurrence.download.downloads_hard_limit


        occurrence.download.mail.smtp
        occurrence.download.mail.from
        occurrence.download.mail.bcc

        appkeys.testfile
        appkeys.file
        appkeys.whitelist
``` 

 2. Copy the reference config files located in [src/test/resources/ref-conf/](src/test/resources/ref-conf/) to
*src/main/resources*.
 3. Run the Maven wrapper command:
 
``` 
  ./mvnw -Poccurrence-dev spring-boot:run
``` 
  

Then test it with [http://localhost:8080/occurrence/search](http://localhost:8080/occurrence/search)

## Downloads
This is the webservice that orchestrates occurrence downloads by accepting a JSON download string, translating it
into a Hive/SOLR download, kicking off the Oozie workflow that calls Hive/SOLR, and then responding to the original request
with the location of the download.


Testing it
----------

An example JSON file as input to the service is included in src/test/resources and it can be posted to this
webservice using curl as below:

```
curl -X POST --header "Content-Type:application/json" --user username:password -d @sample_taxon_key.json http://localhost:8080/occurrence/download/request
```
