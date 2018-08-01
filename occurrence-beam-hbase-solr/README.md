# Occurrence Beam HBase SOLR

Uses Apache Beam to read HBase and index into SOLR through HTTP.
This is a replacement for the HBase->Morphline->Avro->MapReduce->GoLive process and makes better use of the real-time indexing code path.

#### Set up SOLR
Using the Solr directory from the `occurrence-index-builder-workflow` create a collection - e.g.
```
scp -r solr trobertson@c4gateway-vh.gbif.org:/tmp/solr-uat

/opt/cloudera/parcels/CDH-5.12.1-1.cdh5.12.1.p0.3/lib/solr/bin/zkcli.sh -zkhost c4master1-vh.gbif.org:2181,c4master2-vh.gbif.org:2181,c4master3-vh.gbif.org:2181/solrp -cmd upconfig -confname tim-occurrence -confdir ./solr-uat/collection1/conf
solrctl --solr http://c4n1.gbif.org:8983/solr --zk c4master1-vh.gbif.org:2181,c4master2-vh.gbif.org:2181,c4master3-vh.gbif.org:2181/solrp collection --create tim-occurrence -s 36 -c occurrence -r 1 -m 4

```

#### Run the Solr index
The following is used on the UAT environment (6.5hrs for 1B records). Note that with 1 replicas (i.e. no copies) it is 2x faster than with 2 replicas. Solr _appears_ to be more stable in writing in this configuration as well. 
```
scp target/occurrence-beam-hbase-solr-0.84-SNAPSHOT-shaded.jar trobertson@c4gateway-vh.gbif.org:.
nohup sudo -u hdfs spark-submit --conf spark.default.parallelism=1000 --conf spark.yarn.executor.memoryOverhead=2048 --class org.gbif.occurrence.beam.solr.BulkloadSolr --master yarn --executor-memory 4G --executor-cores 4 --num-executors 18 /home/trobertson/occurrence-beam-hbase-solr-0.84-SNAPSHOT-shaded.jar --runner=SparkRunner &
```

### Add replicas
Adding replicas requires that you name the target node (or else it CAN put them on the same machine):
```
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=MODIFYCOLLECTION&collection=tim-occurrence&maxShardsPerNode=8'

curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard1&node=c4n3.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard2&node=c4n4.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard3&node=c4n7.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard4&node=c4n8.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard5&node=c4n1.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard6&node=c4n2.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard7&node=c4n9.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard8&node=c4n6.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard9&node=c4n5.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard10&node=c4n3.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard11&node=c4n4.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard12&node=c4n7.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard13&node=c4n8.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard14&node=c4n1.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard15&node=c4n2.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard16&node=c4n9.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard17&node=c4n6.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard18&node=c4n5.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard19&node=c4n3.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard20&node=c4n4.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard21&node=c4n7.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard22&node=c4n8.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard23&node=c4n1.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard24&node=c4n2.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard25&node=c4n9.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard26&node=c4n6.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard27&node=c4n5.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard28&node=c4n3.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard29&node=c4n4.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard30&node=c4n7.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard31&node=c4n8.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard32&node=c4n1.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard33&node=c4n2.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard34&node=c4n9.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard35&node=c4n6.gbif.org:8983_solr'
curl 'http://c4n1.gbif.org:8983/solr/admin/collections?action=ADDREPLICA&collection=tim-occurrence&shard=shard36&node=c4n5.gbif.org:8983_solr'
```

The Solr logs might show the following which can be ignored:
```
PeerSync: core=tim-occurrence_shard9_replica2 url=http://c4n5.gbif.org:8983/solr ERROR, update log not in ACTIVE or REPLAY state. HDFSUpdateLog{state=BUFFERING, tlog=null}
```

Seemingly the replicas will be rebuilt as new lucene indexes on each machine taking >1hr. During this time Solr cloud will show `recovering` on the shards, and CPU and network will be reasonably high on the cluster. 
