#!/usr/bin/env bash

#exit on any failure
set -e
set -o pipefail

P=$1
TOKEN=$2
SOURCE_DIR=${3:-hdfs://ha-nn/data/hdfsview/occurrence/}
TABLE_NAME=${4:-occurrence}
SCHEMA_CHANGED=${5:-true}
TABLE_SWAP=${6:-true}


echo "Get latest tables-coord config profiles from github"
curl -s -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/occurrence-download/profiles.xml

NAME_NODE=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="'$P'"]]/*[name()="properties"]/*[name()="hdfs.namenode"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')
ENV=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="'$P'"]]/*[name()="properties"]/*[name()="occurrence.environment"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')
OOZIE=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="'$P'"]]/*[name()="properties"]/*[name()="oozie.url"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')
HIVE_DB=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="'$P'"]]/*[name()="properties"]/*[name()="hive.db"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')

echo "Assembling jar for $ENV"
#Oozie uses timezone UTC
mvn --settings profiles.xml -U -P$P -DskipTests -Duser.timezone=UTC clean install package assembly:single

java -classpath "target/occurrence-download-workflows-$ENV/lib/*" org.gbif.occurrence.download.conf.DownloadConfBuilder $P  target/occurrence-download-workflows-$ENV/lib/occurrence-download.properties profiles.xml
echo "Copy to hadoop"
sudo -u hdfs hdfs dfs -rm -r -f /occurrence-download-workflows-new-schema-$ENV/
sudo -u hdfs hdfs dfs -mkdir  /occurrence-download-workflows-new-schema-$ENV/
sudo -u hdfs hdfs dfs -copyFromLocal target/occurrence-download-workflows-$ENV/*  /occurrence-download-workflows-new-schema-$ENV/
echo -e "oozie.use.system.libpath=true\noozie.launcher.mapreduce.user.classpath.first=true\noozie.wf.application.path=$NAME_NODE/occurrence-download-workflows-new-schema-$ENV/create-tables\nhiveDB=$HIVE_DB\noozie.libpath=/occurrence-download-workflows-new-schema-$ENV/lib/,/user/oozie/share/lib/gbif/hive\noozie.launcher.mapreduce.task.classpath.user.precedence=true\nuser.name=hdfs\nenv=$ENV\nsource_data_dir=$SOURCE_DIR\ndownload_table_name=$TABLE_NAME\nschema_change=$SCHEMA_CHANGED\ntable_swap=$TABLE_SWAP"  > job.properties

sudo -u hdfs oozie job --oozie $OOZIE -config job.properties -run
