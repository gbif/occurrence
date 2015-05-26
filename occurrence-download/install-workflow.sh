#exit on any failure
set -e

#!/bin/bash
P=$1
TOKEN=$2

echo "Get latest tables-coord config profiles from github"
curl -s -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/occurrence-download/profiles.xml

NAME_NODE=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="$P"]]/*[name()="properties"]/*[name()="hdfs.namenode"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')
ENV=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="$P"]]/*[name()="properties"]/*[name()="occurrence.environment"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')
OOZIE=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="$P"]]/*[name()="properties"]/*[name()="oozie.url"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')
HBASE_TABLE=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="$P"]]/*[name()="properties"]/*[name()="hbase.table"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')
HIVE_DB=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="$P"]]/*[name()="properties"]/*[name()="hive.db"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')

#gets the oozie id of the current coordinator job if it exists
WID=$(oozie jobs -oozie $OOZIE -jobtype coordinator -filter name=OccurrenceHDFSBuild-$ENV\;status=RUNNING\;status=PREP\;status=PREPSUSPENDED\;status=SUSPENDED\;status=PREPPAUSED\;status=PAUSED\;status=SUCCEEDED\;status=DONEWITHERROR\;status=FAILED |  awk 'NR==3' | awk '{print $1;}')
if [ -n "$WID" ]; then
  echo "Killing current coordinator job" $WID
  oozie job -oozie $OOZIE -kill $WID
fi

echo "Assembling jar for $ENV"
#Oozie uses timezone UTC
mvn --settings profiles.xml -P$P -DskipTests -Duser.timezone=UTC clean install package assembly:single

java -classpath "target/occurrence-download-workflows-$ENV/lib/*" org.gbif.occurrence.download.conf.DownloadConfBuilder $P  target/occurrence-download-workflows-$ENV/lib/occurrence-download.properties profiles.xml
echo "Copy to hadoop"
hdfs dfs -rm -r /occurrence-download-workflows-$ENV/
hdfs dfs -copyFromLocal target/occurrence-download-workflows-$ENV/ /
echo -e "oozie.use.system.libpath=true\noozie.coord.application.path=$NAME_NODE/occurrence-download-workflows-$ENV/create-tables\nhiveDB=$HIVE_DB\noccurrenceHBaseTable=$HBASE_TABLE\noozie.libpath=/occurrence-download-workflows-$ENV/lib/\noozie.launcher.mapreduce.task.classpath.user.precedence=true\nuser.name=occurrence-download"  > job.properties

oozie job --oozie $OOZIE -config job.properties -run

