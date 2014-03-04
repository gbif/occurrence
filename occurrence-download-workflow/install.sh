#!/bin/bash
ENV=$1
P=$2

mvn -P$P clean package assembly:single -Doccurrence.download.ws.password=$3
hadoop dfs -rm -r -skipTrash /occurrence-download/$ENV
hadoop dfs -put target/oozie-workflow /occurrence-download/$ENV
