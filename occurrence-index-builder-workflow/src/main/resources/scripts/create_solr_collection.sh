#!/usr/bin/env bash
ZK_HOST=$1
SOLR_COLLECTION=$2
SOLR_COLLECTION_OPTS=$3
SOLR_HTTP_URL=$4

sudo -u hdfs

curl """$SOLR_HTTP_URL"/admin/collections?action=DELETE\&name="$SOLR_COLLECTION"""
/opt/cloudera/parcels/SOLR5/server/scripts/cloud-scripts/zkcli.sh -zkhost $ZK_HOST -cmd upconfig -confname $SOLR_COLLECTION -confdir solr/collection1/conf/
curl """$SOLR_HTTP_URL"/admin/collections?action=CREATE\&name="$SOLR_COLLECTION"\&"$SOLR_COLLECTION_OPTS"\&collection.configName="$SOLR_COLLECTION"""
