#c1n1.gbif.org:2181,c1n2.gbif.org:2181,c1n3.gbif.org:2181/solr
ZK_HOST=$1
#occurrence
SOLR_COLLECTION=$2
export HADOOP_CLASSPATH=${maven.dependency.org.gbif.gbif-api.jar}:occurrence-common-0.18-SNAPSHOT.jar:occurrence-search-0.18-SNAPSHOT.jar:/opt/cloudera/parcels/SOLR/lib/hbase-solr/lib/*
hadoop --config /etc/hadoop/conf jar /opt/cloudera/parcels/SOLR/lib/hbase-solr/tools/hbase-indexer-mr-*-job.jar --conf /etc/hbase/conf/hbase-site.xml -D 'mapred.child.java.opts=-Xmx500m' -libjars /opt/cloudera/parcels/SOLR/lib/solr/server/webapps/solr/WEB-INF/lib/jts-1.13.jar,gbif-api-0.14-SNAPSHOT.jar,occurrence-common-0.18-SNAPSHOT.jar,occurrence-search-0.18-SNAPSHOT.jar --hbase-indexer-file hbase_occurrence_batch_morphline.xml --zk-host $ZK_HOST --collection $SOLR_COLLECTION --go-live --log4j /opt/cloudera/parcels/SOLR/share/doc/search-1.2.0/examples/solr-nrt/log4j.properties