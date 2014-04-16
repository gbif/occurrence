#exit on any failure
set -e

#!/bin/bash
ENV=$1
P=$2
OOZIE=$3

#gets the oozie id of the current coordinator job if it exists
WID=$(oozie jobs -oozie $OOZIE -jobtype coordinator -filter name=OccurrenceHDFSBuild-$ENV\;status=RUNNING |  awk 'NR==3' | awk '{print $1;}')
if [ -n "$WID" ]; then
  echo "Killing current coordinator job" $WID
  oozie job -oozie $OOZIE -kill $WID
fi

mvn -P$P clean package assembly:single
hadoop fs -rm -r -skipTrash /occurrence-tables-coord/$ENV
hadoop fs -put target/oozie-workflow /occurrence-tables-coord/$ENV
oozie job --oozie $OOZIE -config target/oozie-workflow/job.properties -run
