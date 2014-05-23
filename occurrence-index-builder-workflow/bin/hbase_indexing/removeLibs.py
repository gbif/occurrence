from fabric.api import env
from fabric.operations import run
from fabric.context_managers import cd

env.hosts = ['root@c2n1.gbif.org','root@c2n2.gbif.org','root@c2n3.gbif.org','root@c1n1.gbif.org','root@c1n2.gbif.org','root@c1n3.gbif.org']

def remove():
    with cd('/opt/cloudera/parcels/SOLR-1.2.0-1.cdh4.5.0.p0.4/lib/hbase-solr/lib'):
                run('rm -f occurrence-hive*.jar')
                run('rm -f occurrence-common*.jar')
                run('rm -f gbif-api*.jar')