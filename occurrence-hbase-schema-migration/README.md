## Occurrence HBase Schema Migration

*This is an interim module that will be removed once data is migrated*

This contains a map reduce job to populate a new hbase occurrence table with a schema migration and modulo salted key.

Build the project: `mvn clean package`

**Example usage from a gateway**

Create the target tables:

```
create 'tim_occurrence',
  {NAME => 'fragment', VERSIONS => 1, COMPRESSION => 'SNAPPY', DATA_BLOCK_ENCODING => 'FAST_DIFF', BLOOMFILTER => 'ROW'},
{SPLITS => [
    '01','02','03','04','05','06','07','08','09','10',
    '11','12','13','14','15','16','17','18','19','20',
    '21','22','23','24','25','26','27','28','29','30',
    '31','32','33','34','35','36','37','38','39','40',
    '41','42','43','44','45','46','47','48','49','50',
    '51','52','53','54','55','56','57','58','59','60',
    '61','62','63','64','65','66','67','68','69','70',
    '71','72','73','74','75','76','77','78','79','80',
    '81','82','83','84','85','86','87','88','89','90',
    '91','92','93','94','95','96','97','98','99'
  ]}

```

Execute on a CDH managed gateway:

```
sudo -u hdfs  HADOOP_CLASSPATH=`${HBASE_HOME}/bin/hbase classpath` \
  hadoop jar ./occurrence-hbase-schema-migration-0.119-ES-SNAPSHOT.jar org.gbif.fixup.occurrence.SchemaMigrationDriver \
  -zk c5master1-vh.gbif.org,c5master2-vh.gbif.org,c5master3-vh.gbif.org \
  -snapshot tim_fragment_test \
  -target tim_occurrence \
  -tmpPath /tmp/schema-rewrite-tmp
```
