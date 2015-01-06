#exit on any failure
set -e

#!/bin/bash
ENV=$1
P=$2
GIT_CREDENTIALS=$3
VERSION=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | grep ^0.`

# we use settings from a provate github repo with secret passwords
if [ -d "gbif-configuration" ]; then
  #If gitrepo exists, update the repositories
  echo "Updating local gbif-configuration Git repository"
  cd gbif-configuration
  git pull --all
  cd ..
else
  echo "Cloning Git repository gbif-configuration"
  git clone https://$GIT_CREDENTIALS@github.com/gbif/gbif-configuration
fi

echo "Assembling workflow jar for $ENV"
mvn --settings gbif-configuration/occurrence-download-workflow/$VERSION/profiles.xml -P$P -DskipTests clean package assembly:single

echo "Copy workflow to hadoop"
hadoop fs -rm -r -skipTrash /occurrence-download/$ENV
hadoop fs -put target/oozie-workflow /occurrence-download/$ENV
echo "Copied to hadoop"
