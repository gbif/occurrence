from fabric.api import env
from fabric.operations import put

env.hosts = ['root@c2n1.gbif.org','root@c2n2.gbif.org','root@c2n3.gbif.org','root@c1n1.gbif.org','root@c1n2.gbif.org','root@c1n3.gbif.org']

def copy():
    put('occurrence-search*.jar', '/opt/cloudera/parcels/SOLR-1.2.0-1.cdh4.5.0.p0.4/lib/hbase-solr/lib')
    put('occurrence-common*.jar', '/opt/cloudera/parcels/SOLR-1.2.0-1.cdh4.5.0.p0.4/lib/hbase-solr/lib')
    put('gbif-api*.jar', '/opt/cloudera/parcels/SOLR-1.2.0-1.cdh4.5.0.p0.4/lib/hbase-solr/lib')