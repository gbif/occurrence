#exit on any failure
set -e

#!/bin/bash
ENV=$1
P=$2
OOZIE=$3
TOKEN=$4
VERSION=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep ^0.`

#gets the oozie id of the current coordinator job if it exists
WID=$(oozie jobs -oozie $OOZIE -jobtype coordinator -filter name=OccurrenceHDFSBuild-$ENV\;status=RUNNING |  awk 'NR==3' | awk '{print $1;}')
if [ -n "$WID" ]; then
  echo "Killing current coordinator job" $WID
  oozie job -oozie $OOZIE -kill $WID
fi

echo "Get latest tables-coord config profiles from github"
curl -s -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/occurrence-tables-coord/$VERSION/profiles.xml

echo "Assembling jar for $ENV"
mvn --settings profiles.xml -P$P -DskipTests clean install package assembly:single

echo "Copy to hadoop"
hadoop fs -rm -r -skipTrash /occurrence-tables-coord/$ENV
hadoop fs -put target/oozie-workflow /occurrence-tables-coord/$ENV
oozie job --oozie $OOZIE -config target/oozie-workflow/job.properties -run
