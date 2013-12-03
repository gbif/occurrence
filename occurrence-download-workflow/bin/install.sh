cd ..
mvn -P$1 clean package assembly:single
hadoop fs -rm -r -skipTrash $2*
hadoop dfs -put target/oozie-workflow/* $2