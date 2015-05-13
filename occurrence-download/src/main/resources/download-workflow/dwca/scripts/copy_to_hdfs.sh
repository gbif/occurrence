#!/bin/bash
workflow_key=$1
source_dir=$2
hdfs_destination_dir=$3

file_name=${workflow_key}.zip

hdfs dfs -put $source_dir/$file_name $hdfs_destination_dir/$file_name
