SOLR_COLLECTION=$2
SOLR_SERVER=$1
ssh root@$SOLR_SERVER '//usr/local/jetty-occurrence/stop-jetty.sh'
ssh root@$SOLR_SERVER 'rm -rf //usr/local/jetty-occurrence/solr/ocurrence/bkpdata'
ssh root@$SOLR_SERVER 'mv -rf //usr/local/jetty-occurrence/solr/ocurrence/data //usr/local/jetty-occurrence/solr/ocurrence/bkpdata'
hadoop fs -copyToLocal /solr/$SOLR_COLLECTION_single/results/part-00000/data //auto/solr/$SOLR_COLLECTION/solr/occurrence/data
ssh root@$SOLR_SERVER '//usr/local/jetty-occurrence/start-jetty.sh'