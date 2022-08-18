#!/usr/bin/env bash

#exit on any failure
set -e
set -o pipefail

P=$1
TOKEN=$2
SOURCE_DIR=${3:-hdfs://ha-nn/data/hdfsview/}
TABLE_NAME=${4:-occurrence}

echo "Get latest tables-coord config profiles from github"
curl -s -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/${TABLE_NAME}-download/profiles.xml

NAME_NODE=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="'$P'"]]/*[name()="properties"]/*[name()="hdfs.namenode"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')
ENV=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="'$P'"]]/*[name()="properties"]/*[name()="occurrence.environment"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')
OOZIE=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="'$P'"]]/*[name()="properties"]/*[name()="oozie.url"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')
HIVE_DB=$(echo 'cat /*[name()="settings"]/*[name()="profiles"]/*[name()="profile"][*[name()="id" and text()="'$P'"]]/*[name()="properties"]/*[name()="hive.db"]/text()' | xmllint --shell profiles.xml | sed '/^\/ >/d' | sed 's/<[^>]*.//g')

echo "Assembling jar for $ENV"
#Oozie uses timezone UTC
mvn --settings profiles.xml -U -P$P -DskipTests -Duser.timezone=UTC clean install package assembly:single

#Is any download running?
while [[ $(curl -Ss --fail "$OOZIE/v1/jobs?filter=status=RUNNING;status=PREP;status=SUSPENDED;name=${ENV}-${TABLE_NAME}-download;name=${ENV}-create-tables" | jq '.workflows | length') > 0 ]]; do
  echo -e "$(tput setaf 1)Download workflow can not be installed while download or create HDFS table workflows are running!!$(tput sgr0) \n"
  oozie jobs -oozie $OOZIE -jobtype wf -filter "status=RUNNING;status=PREP;status=SUSPENDED;name=${ENV}-${TABLE_NAME}-download;name=${ENV}-create-tables"
  sleep 5
done

#gets the oozie id of the current coordinator job if it exists
WID=$(oozie jobs -oozie $OOZIE -jobtype coordinator -filter name=${TABLE_NAME}-HDFSBuild-$ENV | awk 'NR==3 {print $1}')
if [ -n "$WID" ]; then
  echo "Killing current coordinator job" $WID
  sudo -u hdfs oozie job -oozie $OOZIE -kill $WID
fi

java -classpath "target/${TABLE_NAME}-download-workflows-$ENV/lib/*" org.gbif.occurrence.download.conf.DownloadConfBuilder $P  target/${TABLE_NAME}-download-workflows-$ENV/lib/download.properties profiles.xml
echo "Copy to hadoop"
sudo -u hdfs hdfs dfs -rm -r /${TABLE_NAME}-download-workflows-$ENV/ || echo "No old workflow to remove"
sudo -u hdfs hdfs dfs -copyFromLocal target/${TABLE_NAME}-download-workflows-$ENV/ /
echo -e "oozie.use.system.libpath=true\noozie.launcher.mapreduce.user.classpath.first=true\noozie.coord.application.path=$NAME_NODE/${TABLE_NAME}-download-workflows-$ENV/create-tables\nhiveDB=$HIVE_DB\noozie.libpath=/${TABLE_NAME}-download-workflows-$ENV/lib/,/user/oozie/share/lib/gbif/hive\noozie.launcher.mapreduce.task.classpath.user.precedence=true\nuser.name=hdfs\nenv=$ENV\nsource_data_dir=$SOURCE_DIR\ntable_name=${TABLE_NAME}\nschema_change=false\ntable_swap=false"  > job.properties

sudo -u hdfs oozie job --oozie $OOZIE -config job.properties -run
