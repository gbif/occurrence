Fabric commands
---------------
This directory contains fabric scripts to install and configure the Solr 5.3.0 libraries in a Hadoop cluster


## Commands

* install_solr_dev and install_solr_prod: downloads, extracts and copies Solr 5.3.0 into /opt/ directory.
* copy_set_mr_path_dev and copy_set_mr_path_prod: copies the file set-map-reduce-classpath.sh into /opt/solr-5.3.0/server/scripts/map-reduce/; this file has been modified to exclude hadoop libraries from the classpath.


