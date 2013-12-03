#!/bin/bash
./hadoop dfs -rmr /occurrence-download/v1
./hadoop dfs -put /Users/mdoering/dev/occurrence-download/occurrence-download-workflow/target/occurrence-download-workflow-0.5-SNAPSHOT-oozie /occurrence-download/v1
