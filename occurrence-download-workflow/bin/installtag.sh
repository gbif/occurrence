svn co https://gbif-occurrencestore.googlecode.com/svn/occurrence-download/tags/$3/occurrence-download-workflow/ occurrence-download-workflow
cd occurrence-download-workflow
mvn -P$1 clean package assembly:single
hadoop fs -rm -r -skipTrash $2*
hadoop dfs -put target/oozie-workflow/* $2
cd ..
rm -rf occurrence-download-workflow