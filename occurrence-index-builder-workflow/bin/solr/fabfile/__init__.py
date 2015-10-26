#fab install_solr_dev -u root

from fabric.api import *

env.roledefs['cluster_dev'] = ['c1n1.gbif.org','c1n2.gbif.org','c1n3.gbif.org','c2n1.gbif.org','c2n2.gbif.org','c2n3.gbif.org']

env.roledefs['cluster_prod'] = ['c4n1.gbif.org','c4n2.gbif.org','c4n3.gbif.org','c4n4.gbif.org','c4n5.gbif.org','c4n6.gbif.org','c4n7.gbif.org','c4n8.gbif.org','c4n9.gbif.org','c4n10.gbif.org','c4n11.gbif.org','c4n12.gbif.org','prodmaster1-vh.gbif.org','prodmaster2-vh.gbif.org','prodmaster3-vh.gbif.org']

env.roledefs['solr_uat'] = ['uatsolr1-vh.gbif.org','uatsolr2-vh.gbif.org','uatsolr3-vh.gbif.org','uatsolr4-vh.gbif.org','uatsolr5-vh.gbif.org']


@task
@parallel
@roles('cluster_dev')
def install_solr_dev():
  install_solr()


@task
@parallel
@roles('cluster_prod')
def install_solr_prod():
  install_solr()


def install_solr():
  run('''rm -rf /opt/solr-5.3.0/;\
        cd /tmp/;\
        wget http://mirrors.dotsrc.org/apache/lucene/solr/5.3.0/solr-5.3.0.tgz;\
        tar xzf solr-5.3.0.tgz -C /opt/;\
        rm -f solr-5.3.0.tgz;''')
  copy_set_mr_path()

def copy_set_mr_path():
  put('set-map-reduce-classpath.sh','/opt/solr-5.3.0/server/scripts/map-reduce/',mode=0755)


@task
@parallel
@roles('cluster_dev')
def copy_set_mr_path_dev():
  copy_set_mr_path()


@task
@parallel
@roles('cluster_prod')
def copy_set_mr_path_prod():
  copy_set_mr_path()

@task
@parallel
def copy_file(file,target_dir):
  put(file,target_dir,mode=0755)


@task
@parallel
def start_solr():
  run('''service solr start;''')

@task
@parallel
def stop_solr():
  run('''service solr stop;''')

@task
@parallel
def restart_solr():
  run('''service solr restart;''')
