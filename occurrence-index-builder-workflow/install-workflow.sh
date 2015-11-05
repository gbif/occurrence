#exit on any failure
set -e

#!/bin/bash
P=$1
TOKEN=$2

echo "Getting latest occurrence-index-builder workflow properties file from github"
curl -s -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/occurrence-index-builder/$P.properties
echo "Get latest tables-coord config profiles from github"
curl -s -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/occurrence-download/profiles.xml

#extract the oozie.url value from the properties file
oozie_url=`cat $P.properties| grep "oozie.url" | cut -d'=' -f2`

echo "Assembling jar for $ENV"

mvn --settings profiles.xml -Poozie,$P clean package assembly:single
mvn --settings profiles.xml -Psolr,$P package assembly:single

if hdfs dfs -test -d /occurrence-index-builder-$P/; then
   echo "Removing content of current Oozie workflow directory"
   hdfs dfs -rm -r -f /occurrence-index-builder-$P/*
else
   echo "Creating workflow directory"
   hdfs dfs -mkdir /occurrence-index-builder-$P/
fi
echo "Copying new Oozie workflow to HDFS"
hdfs dfs -copyFromLocal target/oozie-workflow/* /occurrence-index-builder-$P/

echo "Executing Oozie workflow"
oozie job --oozie ${oozie_url} -config $P.properties -run

