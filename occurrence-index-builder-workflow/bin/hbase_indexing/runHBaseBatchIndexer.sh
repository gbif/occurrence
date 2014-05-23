#c1n1.gbif.org:2181,c1n2.gbif.org:2181,c1n3.gbif.org:2181/solr
ZK_HOST=$1
#occurrence
SOLR_COLLECTION=$2
export HADOOP_CLASSPATH=gbif-api-${gbif-api.version}.jar:occurrence-common-${parent.version}.jar:occurrence-search-${parent.version}.jar:/opt/cloudera/parcels/SOLR/lib/hbase-solr/lib/*
hadoop --config /etc/hadoop/conf jar /opt/cloudera/parcels/SOLR/lib/hbase-solr/tools/hbase-indexer-mr-*-job.jar --conf /etc/hbase/conf/hbase-site.xml -D 'mapred.child.java.opts=-Xmx500m' -libjars /opt/cloudera/parcels/SOLR/lib/solr/server/webapps/solr/WEB-INF/lib/jts-1.13.jar,gbif-api-${gbif-api.version}.jar,occurrence-common-${parent.version}.jar,occurrence-search-${parent.version}.jar --hbase-indexer-file hbase_occurrence_batch_morphline.xml --zk-host $ZK_HOST --collection $SOLR_COLLECTION --go-live --log4j log4j.properties