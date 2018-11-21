# GBIF Occurrence Web Services

These web services provide two important parts of GBIF.org: the occurrence details themselves, and the download service. It is typically
deployed once, but then accessed through two different Varnish backends – one for occurrence details and one for downloads (in order to
isolate the often slow download threads from the fast occurrence detail lookups).

## Building
To produce the artifact that will be deployed just run `mvn clean package`. This will strip all local configs and expects that you
provide configs that match your destination environment (i.e. dev, uat, or prod – get the configs from gbif-configuration).

## Usage
For local development and testing you can run this project using `mvn -Pdev jetty:run`. You will need to configure a local dev profile like the following, and you need to provide a maven profile that contains these two parameters:
  1. occurrence.download.ws.password
  2. drupal.db.password (out of date!)

The dev profile from http://github.com/gbif/gbif-configuration/maven/settings.xml should do the job.

Then test it with `http://localhost:8080/occurrence/12345`

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
