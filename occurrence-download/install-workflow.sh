#exit on any failure
set -e

#!/bin/bash
P=$1
TOKEN=$2


echo "Get latest tables-coord config profiles from github"
curl -s -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/occurrence-download/profiles.xml

NAME_NODE=$(xmllint --xpath '/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="settings"]/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="profiles"]/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="profile"][*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="id" and text()="dev"]]/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="properties"]/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="hdfs.namenode"]/text()' profiles.xml)
ENV=$(xmllint --xpath '/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="settings"]/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="profiles"]/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="profile"][*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="id" and text()="dev"]]/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="properties"]/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="occurrence.environment"]/text()' profiles.xml)
OOZIE=$(xmllint --xpath '/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="settings"]/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="profiles"]/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="profile"][*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="id" and text()="dev"]]/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="properties"]/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="oozie.url"]/text()' profiles.xml)
HBASE_TABLE=$(xmllint --xpath '/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="settings"]/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="profiles"]/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="profile"][*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="id" and text()="dev"]]/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="properties"]/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="hbase.table"]/text()' profiles.xml)
HIVE_DB=$(xmllint --xpath '/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="settings"]/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="profiles"]/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="profile"][*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="id" and text()="dev"]]/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="properties"]/*[namespace-uri()="http://maven.apache.org/SETTINGS/1.0.0" and name()="hive.db"]/text()' profiles.xml)


#gets the oozie id of the current coordinator job if it exists
WID=$(oozie jobs -oozie $OOZIE -jobtype coordinator -filter name=OccurrenceHDFSBuild-$ENV\;status=RUNNING |  awk 'NR==3' | awk '{print $1;}')
if [ -n "$WID" ]; then
  echo "Killing current coordinator job" $WID
  oozie job -oozie $OOZIE -kill $WID
fi

echo "Assembling jar for $ENV"
mvn --settings profiles.xml -P$P -DskipTests clean install package assembly:single

echo "Copy to hadoop"
hdfs dfs -rm -r /occurrence-download-workflows-$ENV/
hdfs dfs -copyFromLocal target/occurrence-download-workflows-$ENV/ /
echo -e "oozie.use.system.libpath=true\noozie.coord.application.path=$NAME_NODE/occurrence-download-workflows-$ENV/create-tables\nhiveDB=$HIVE_DB\noccurrenceHBaseTable=$HBASE_TABLE"  > job.properties

oozie job --oozie $OOZIE -config job.properties -run


