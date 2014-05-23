#c1n1.gbif.org:2181,c1n2.gbif.org:2181,c1n3.gbif.org:2181
ZK_HOST=$1
SOLR_COLLECTION=&2
hbase-indexer add-indexer --name occurrenceIndexer --indexer-conf hbase_occurrence_nrt_morphline.xml --connection-param solr.zk=${ZK_HOST}/solr --connection-param solr.collection=$2 --zookeeper $ZK_HOST