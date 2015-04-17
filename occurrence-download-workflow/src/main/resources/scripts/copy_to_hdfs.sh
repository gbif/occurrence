#!/bin/bash
workflow_id=$1
source_dir=$2
hdfs_destination_dir=$3

oozie_suffix='-oozie-oozi-W'
file_name=${workflow_id//$oozie_suffix/}.zip

hdfs dfs -put $source_dir/$file_name $hdfs_destination_dir/$file_name
