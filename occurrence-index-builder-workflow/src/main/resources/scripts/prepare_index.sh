solrctl collection --delete $1
solrctl instancedir --delete $1

solrctl instancedir --create $1 solr/occurrence
solrctl collection --create $1 $2