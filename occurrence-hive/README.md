# GBIF Occurrence Hive

A separate module to hold the Hive User Defined Functions (UDFs) currently only used by the analytics processing. To build this JAR it needs to know where to find the NUB lookup and geocode webservices, like so:

````shell
mvn clean package -Dchecklistbank.match.ws.url=http://api.gbif-uat.org/v0.9/species/match -Dgeocode.ws.url=http://api.gbif-uat.org/v0.9/lookup/reverse_geocode
````
