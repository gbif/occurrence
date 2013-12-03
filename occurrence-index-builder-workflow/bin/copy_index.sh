. $1
hadoop fs -copyToLocal -ignoreCrc ${nameNode}/solr/${solrCollection}_single/results/part-00000/* .
scp -r data/ ${targetServerPath}
rm -rf data/