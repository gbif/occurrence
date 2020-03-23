#!/usr/bin/env bash

#exit on any failure
set -e
set -o pipefail

P=$1
TOKEN=$2
SOURCE_DIR=${3:-hdfs://ha-nn/data/hdfsview/occurrence/}
TABLE_NAME=${4:-occurrence_pipeline}

echo "Get latest tables-coord config profiles from github"
curl -s -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/occurrence-download/profiles.xml

NAME_NODE=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="'$P'"]]/*[name()="properties"]/*[name()="hdfs.namenode"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')
ENV=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="'$P'"]]/*[name()="properties"]/*[name()="occurrence.environment"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')
OOZIE=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="'$P'"]]/*[name()="properties"]/*[name()="oozie.url"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')
HIVE_DB=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="'$P'"]]/*[name()="properties"]/*[name()="hive.db"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')

#gets the oozie id of the current coordinator job if it exists
WID=$(oozie jobs -oozie $OOZIE -jobtype coordinator -filter name=OccurrencePipelinesHDFSBuild-$ENV | awk 'NR==3 {print $1}')
if [ -n "$WID" ]; then
  echo "Killing current coordinator job" $WID
  sudo -u hdfs oozie job -oozie $OOZIE -kill $WID
fi

echo "Assembling jar for $ENV"
#Oozie uses timezone UTC
mvn --settings profiles.xml -U -P$P -DskipTests -Duser.timezone=UTC clean install package assembly:single

java -classpath "target/occurrence-download-workflows-$ENV/lib/*" org.gbif.occurrence.download.conf.DownloadConfBuilder $P  target/occurrence-download-workflows-$ENV/lib/occurrence-download.properties profiles.xml
echo "Copy to hadoop"
sudo -u hdfs hdfs dfs -rm -r /occurrence-download-workflows-$ENV/
sudo -u hdfs hdfs dfs -copyFromLocal target/occurrence-download-workflows-$ENV/ /
echo -e "oozie.use.system.libpath=true\noozie.launcher.mapreduce.user.classpath.first=true\noozie.coord.application.path=$NAME_NODE/occurrence-download-workflows-$ENV/create-tables\nhiveDB=$HIVE_DB\noozie.libpath=/occurrence-download-workflows-$ENV/lib/,/user/oozie/share/lib/gbif/hive\noozie.launcher.mapreduce.task.classpath.user.precedence=true\nuser.name=hdfs\nenv=$ENV\nsource_data_dir=$SOURCE_DIR\ndownload_table_name=occurrence_pipeline\nschema_change=false"  > job.properties

sudo -u hdfs oozie job --oozie $OOZIE -config job.properties -run
