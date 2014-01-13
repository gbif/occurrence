#!/bin/bash
ENV=appdev
P=dev

mvn -P$P clean package assembly:single
#hadoop dfs -rmr /occurrence-download/$ENV
#hadoop dfs -put ../target/oozie-workflow /occurrence-download/$ENV
