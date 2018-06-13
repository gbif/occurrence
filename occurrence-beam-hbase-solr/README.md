# Occurrence Beam HBase SOLR

## EXPERIMENTAL! ##

Uses Apache Beam to read HBase and index into SOLR through HTTP.
This is a potential replacement for the Hbase->Morphline->Avro->MapReduce->GoLive process.

#### Set up SOLR
```
scp -r solr trobertson@c4gateway-vh.gbif.org:/tmp/solr-uat
/opt/cloudera/parcels/CDH-5.12.1-1.cdh5.12.1.p0.3/lib/solr/bin/zkcli.sh -zkhost c4master1-vh.gbif.org:2181,c4master2-vh.gbif.org:2181,c4master3-vh.gbif.org:2181/solrp -cmd upconfig -confname tim-occurrence -confdir ./solr-uat/collection1/conf

solrctl --solr http://c4n1.gbif.org:8983/solr --zk c4master1-vh.gbif.org:2181,c4master2-vh.gbif.org:2181,c4master3-vh.gbif.org:2181/solrp collection --delete tim-occurrence

sudo hdfs dfs -rmr /solrp/tim-occurrence

solrctl --solr http://c4n1.gbif.org:8983/solr --zk c4master1-vh.gbif.org:2181,c4master2-vh.gbif.org:2181,c4master3-vh.gbif.org:2181/solrp collection --create tim-occurrence -s 18 -c tim-occurrence -r 1 -m 2


```

#### Run
scp target/occurrence-beam-hbase-solr-0.83-SNAPSHOT-shaded.jar trobertson@c4gateway-vh.gbif.org:.

nohup sudo -u hdfs spark-submit --conf spark.default.parallelism=1000 --conf spark.yarn.executor.memoryOverhead=2048 --class org.gbif.occurrence.beam.solr.BulkloadSolr --master yarn --executor-memory 4G --executor-cores 5 --num-executors 18 /home/trobertson/occurrence-beam-hbase-solr-0.83-SNAPSHOT-shaded.jar --runner=SparkRunner &
