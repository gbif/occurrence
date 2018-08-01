# -*- mode: sh; -*-
#
# At the moment, some records are missing from the completed SOLR index.
# We find roughly where they are, and reinterpret them after changing the occurrence-cli (IndexUpdaterCallback) to index UNCHANGED records in the same way as UPDATED.
#
# (Written in Zsh, not tested in Bash.)
#

# First, find the counts by 100,000 and 10,000 from HBase:
# CREATE TABLE matt.by_100k ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' AS SELECT COUNT(*), FLOOR(gbifid / 100000) * 100000 FROM prod_e.occurrence_hbase GROUP BY FLOOR(gbifid / 100000);
# CREATE TABLE matt.by_10k ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' AS select count(*), FLOOR(gbifid / 10000) * 10000 FROM prod_e.occurrence_hbase GROUP BY FLOOR(gbifid / 10000);
# hdfs dfs -getmerge /user/hive/warehouse/matt.db/by_100k by_100k.tsv
# hdfs dfs -getmerge /user/hive/warehouse/matt.db/by_10k by_10k.tsv
# And download, copy to 100k and 10k dirs.

# Find gaps by 100,000 records. (Change 2e9 to be above the highest occurrence id.)
mkdir 100k; cd 100k;
for low in `seq 0 100000 2e9`; do
  high=$((low + 99999))
  [[ -f ${low}-${high} ]] || curl -Ssg 'http://c5n1.gbif.org:8983/solr/occurrence_2018_07_08/select?wt=json&rows=0&q=key:['$low'+TO+'$high']' | jq .response.numFound > ${low}-${high}
  echo $(cat ${low}-$high)$'\t'$low
done | grep -v ^0$'\t' | tee result-solr

join -1 2 -2 2 <(sort -k 2b,2 by_100k.tsv) <(sort -k 2b,2 result-solr) | sort -n | awk '$2 != $3 { print $1 }' >! bad-range-starts-100k

# Then search further for the 10k gaps
mkdir 10k; cd 10k
for ll in $(cat ../100k/bad-range-starts-100k); do
  hh=$(($ll + 99999))
  for low in `seq $ll 10000 $hh`; do
    high=$((low + 9999))
    [[ -f ${low}-${high} ]] || curl -Ssg 'http://c5n1.gbif.org:8983/solr/occurrence_2018_07_08/select?wt=json&rows=0&q=key:['$low'+TO+'$high']' | jq .response.numFound > ${low}-${high}
    echo $(cat ${low}-$high)'\t'$low
  done
done | grep -v '^0\t' | tee result-solr
join -1 2 -2 2 <(sort -k 2b,2 by_10k.tsv) <(sort -k 2b,2 result-solr) | sort -n | awk '$2 != $3 { print $1 }' >! bad-range-starts-10k

# Then list the ids in these ranges, the CLI will ignore the missing ones
for ll in $(cat bad-range-starts-10k); do
  for id in `seq $ll $(($ll + 9999))`; do
    echo $id
  done
done | split -n r/3 - bad-ids-

scp bad-ids-aa crap@prodcrawler1-vh.gbif.org: && scp bad-ids-ab crap@prodcrawler2-vh.gbif.org: && scp bad-ids-ac crap@prodcrawler3-vh.gbif.org;

# On CLIs:
cd util
./interpret-occurrences -q 750000 ~/bad-ids-*

# And wait a while so SOLR can resync, then remove the 10k cached counts and results
rm -f *-*
# and the updated 100k cached counts:
for i in $(cat bad-range-starts); do
  rm $i-* -fv
done
rm -f bad-range-starts result-solr
# and redo.
