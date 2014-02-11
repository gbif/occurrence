hbase-migration
===============
The scripts in this folders migrate base table from the table format used before the occurrence widening refactor.
The scripts use 2 variables that must be defined in hive as shown below:
set hivevar:hbase_src_table=test_srv_occurrence;
set hivevar:hbase_target_table=test_target_occurrence;


hbase_src_table: table to copy from
hbase_target_table: target base table