PROFILE=$1
cd ../../
mvn -Poozie,$PROFILE clean package assembly:single
mvn -Psolr,$PROFILE package assembly:single
cd bin/hdfs_indexing/
cat jobsingleshard.properties | sed 's/\./_/g' > .properties.file
. .properties.file
rm .properties.file
echo "Cleaning HDFS directory $oozieWfDestination"
hadoop fs -rm -r -skipTrash $oozieWfDestination*
echo "Copying workflow to HDFS directory $oozieWfDestination"
hadoop dfs -put ../../target/oozie-workflow/* $oozieWfDestination
sudo -u hdfs hadoop fs -chmod -R 0755 /occurrence-index-workflow-single/
oozie_server=${oozie_server//[_]/.}
echo "Running oozie workflow on server $oozie_server"
oozie job -oozie $oozie_server -config jobsingleshard.properties -run
