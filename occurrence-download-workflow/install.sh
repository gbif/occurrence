#exit on any failure
set -e

#!/bin/bash
ENV=$1
P=$2

mvn -P$P clean package assembly:single -Doccurrence.download.ws.password=$3 --offline
hadoop fs -rm -r -skipTrash /occurrence-download/$ENV
hadoop fs -put target/oozie-workflow /occurrence-download/$ENV
