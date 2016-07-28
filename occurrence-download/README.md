# GBIF Occurrence Download

This project provides the Oozie workflows used to:

1. create a snapshot of the HBase table as an HDFS stored Hive table and create views as HDFS tables used during download
2. download through GBIF.org using the occurrence-ws

Note that if the occurrence HDFS table require a schema change, the table must be dropped manually.
