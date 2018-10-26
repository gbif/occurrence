""" Script that add missing replicas to a Solr Collection.
"""

import wget, os,  requests, sys, argparse


def getCollectionInfo(solrServer, collectionName):
  return requests.get(solrServer + "admin/collections?action=clusterstatus&wt=json").json()['cluster']['collections'][collectionName]

def addReplicas(solrServer, collectionName):
  collectionData = getCollectionInfo(solrServer, collectionName)
  replicationFactor = int(collectionData['replicationFactor'])
  for shard in collectionData['shards'].items():
    numberOfCurrentReplicas = len(shard[1]['replicas'])
    if numberOfCurrentReplicas < replicationFactor:
      for x in range(numberOfCurrentReplicas, replicationFactor):
        print requests.get(solrServer + "admin/collections?&wt=json&action=ADDREPLICA&collection=" + collectionName + "&shard=" + shard[0]).text

if __name__ == '__main__':
  #arguments parsing
  parser = argparse.ArgumentParser()
  parser.add_argument("server", help="URL to the Solr server")
  parser.add_argument("collection", help="Solr collection", action="store")
  args = parser.parse_args()
  addReplicas(args.server, args.collection)
  sys.exit(0)


