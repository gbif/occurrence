#exit on any failure
set -e

#!/bin/bash
P=$1
TOKEN=$2
#get the third parameter, if absent use a default of false
IS_SINGLE_SHARD=${3-'false'}

echo "Getting latest occurrence-index-builder workflow properties file from github"
curl -s -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/occurrence-index-builder/$P.properties
echo "Get latest tables-coord config profiles from github"
curl -s -H "Authorization: token $TOKEN" -H 'Accept: application/vnd.github.v3.raw' -O -L https://api.github.com/repos/gbif/gbif-configuration/contents/occurrence-download/profiles.xml


if [ $IS_SINGLE_SHARD = true ] ; then
  echo -e "\nsolr.is_single_shard=true\n" >> $P.properties
else
  echo -e "\nsolr.is_single_shard=false\n" >> $P.properties
fi


#extract the oozie.url value from the properties file
oozie_url=`cat $P.properties| grep "oozie.url" | cut -d'=' -f2`

echo "Assembling jar for $P"

mvn --settings profiles.xml -Poozie,$P clean package assembly:single -U
mvn --settings profiles.xml -Psolr,$P package assembly:single -U

if hdfs dfs -test -d /occurrence-index-builder-$P/; then
   echo "Removing content of current Oozie workflow directory"
   sudo -u hdfs hdfs dfs -rm -r -f /occurrence-index-builder-$P/*
else
   echo "Creating workflow directory"
   sudo -u hdfs hdfs dfs -mkdir /occurrence-index-builder-$P/
fi
echo "Copying new Oozie workflow to HDFS"
sudo -u hdfs hdfs dfs -copyFromLocal target/oozie-workflow/* /occurrence-index-builder-$P/

echo "Executing Oozie workflow"
sudo -u yarn oozie job --oozie ${oozie_url} -config $P.properties -run
