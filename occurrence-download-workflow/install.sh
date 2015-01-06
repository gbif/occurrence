#exit on any failure
set -e

#!/bin/bash
ENV=$1
P=$2
TOKEN=$3
VERSION=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep ^0.`

echo "Get latest workflow config profiles from github"
curl -s -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/occurrence-download-workflow/$VERSION/profiles.xml

echo "Assembling workflow jar for $ENV"
mvn --settings profiles.xml -P$P -DskipTests clean install package assembly:single

echo "Copy workflow to hadoop"
hadoop fs -rm -r -skipTrash /occurrence-download/$ENV
hadoop fs -put target/oozie-workflow /occurrence-download/$ENV
echo "Copied to hadoop"
