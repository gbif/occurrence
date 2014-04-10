#exit on any failure
set -e

#!/bin/bash
ENV=$1
P=$2
OOZIE=$3

mvn -P$P clean package assembly:single
hadoop fs -rm -r -skipTrash /occurrence-tables-coord/$ENV
hadoop fs -put target/oozie-workflow /occurrence-tables-coord/$ENV
oozie job --oozie $OOZIE -config target/oozie-workflow/job.properties -run
