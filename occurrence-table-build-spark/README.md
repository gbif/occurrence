## Occurrence/Event Table Build
Spark Job to crate Occurrence, Event and extensions tables used for downloads and data warehouse in Hive.

```
nohup sudo -u hdfs spark2-submit --class  org.gbif.occurrence.table.backfill.TableBackfill \
  --master yarn --num-executors 100 \
  --executor-cores 6 \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.sql.shuffle.partitions=1000 \
  --executor-memory 4G \
  --driver-memory 2G \
  occurrence-table-build-spark-0.189-hadoop3-SNAPSHOT.jar backfill.yml
```
