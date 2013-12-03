#!/bin/bash
hadoop fs -rmr /occurrence-download/v1/hive-scripts
hadoop fs -rm /occurrence-download/v1/workflow.xml
hadoop fs -rm /occurrence-download/v1/config-default.xml
hadoop fs -rm /occurrence-download/v1/lib/occurrence-download-workflow-0.1-SNAPSHOT.jar
hadoop fs -put target/occurrence-download-workflow-0.1-SNAPSHOT-oozie/hive-scripts /occurrence-download/v1/hive-scripts
hadoop fs -put target/occurrence-download-workflow-0.1-SNAPSHOT-oozie/workflow.xml /occurrence-download/v1/
hadoop fs -put target/occurrence-download-workflow-0.1-SNAPSHOT-oozie/config-default.xml /occurrence-download/v1/
hadoop fs -put target/occurrence-download-workflow-0.1-SNAPSHOT-oozie/lib/occurrence-download-workflow-0.1-SNAPSHOT.jar /occurrence-download/v1/lib/

