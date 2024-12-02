#!/usr/bin/env bash

#exit on any failure
set -e
set -o pipefail

AIRFLOW_USER=$1
AIRFLOW_PWD=$2
MIGRATION=$3

AIRFLOW=http://130.226.238.141:31080

#Is any download running?
while [[ $(curl -Ss --fail -u $AIRFLOW_USER:$AIRFLOW_PWD "$AIRFLOW/api/v1/dags/gbif_occurrence_download_dag/dagRuns?state=running&state=queued" | jq '.dag_runs | length') -gt 0 ||
        $(curl -Ss --fail -u $AIRFLOW_USER:$AIRFLOW_PWD "$AIRFLOW/api/v1/dags/gbif_occurrence_table_build_dag/dagRuns?state=running&state=queued" | jq '.dag_runs | length') -gt 0 ||
        $(curl -Ss --fail -u $AIRFLOW_USER:$AIRFLOW_PWD "$AIRFLOW/api/v1/dags/gbif_occurrence_table_schema_migration_dag/dagRuns?state=running&state=queued" | jq '.dag_runs | length') -gt 0 ]]; do
  echo -e "$(tput setaf 1)Download workflow can not be installed while download or create HDFS table workflows are running!!$(tput sgr0) \n"
  sleep 5
done

if $MIGRATION ; then
  curl -X POST -u $AIRFLOW_USER:$AIRFLOW_PWD -H "Content-Type: application/json" -d '{}' "$AIRFLOW/api/v1/dags/gbif_occurrence_table_schema_migration_dag/dagRuns"
else
  curl -X POST -u $AIRFLOW_USER:$AIRFLOW_PWD -H "Content-Type: application/json" -d '{}' "$AIRFLOW/api/v1/dags/gbif_occurrence_table_build_dag/dagRuns"
fi

