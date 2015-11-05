# GBIF Occurrence Index Builder Workflow

This project contains the oozie workflow that builds and deploy and Occurrence Solr Index from scratch. The assembled
project needs to be copied to hdfs so that each of the task trackers can find the workflow definition and supporting
files. 

To run the oozie workflow the following variables must be provided in a properties file:
  * hive.db: Hive database
  * hadoop.jobtracker
  * hdfs.namenode
  * oozie.url
  * solr.home: directory where Solr libraries are installed (opt/solr-5.3.0/)
  * solr.zk: Solr Zookeeper ensemble, this should include the ZK chroot 
  * solr.http.url=http://uatsolr1-vh.gbif.org:8983/solr
  * hdfs.out.dir: HDFS directory to write  Solr  indexes to
  * solr.collection: name of the Solr cloud collection
  * solr.collection.opts: HTTP parameters used to create the Solr collection, see https://cwiki.apache.org/confluence/display/solr/Collections+API#CollectionsAPI-api1
  * hadoop.client.opts: HADOOP_CLIENT_OPTS, it's used to set memory settings like "-Xmx2048m"
  * mapred.opts: optional mapreduce options, for example: -D 'mapreduce.reduce.shuffle.input.buffer.percent=0.2' -D 'mapreduce.reduce.shuffle.parallelcopies=5' -D 'mapreduce.map.memory.mb=4096' -D 'mapreduce.map.java.opts=-Xmx3584m' -D 'mapreduce.reduce.memory.mb=8192' -D 'mapreduce.reduce.java.opts=-Xmx7680m'
  * oozie.use.system.libpath: default value set to "true"
  * oozie.wf.application.path: HDFS directory where the Oozie workflow is located
  * oozie.libpath: HDFS directory that contains the libraries (jar files and config files) used by the workflow
  * oozie.launcher.mapreduce.task.classpath.user.precedence: default value set to "true"
  * user.name: default set to "yarn" to avoid missing staging file in the jobs triggered from the workflow

  
The project is built following 2 steps:
  1- Create the workflow folder
    mvn -P{environment profile},oozie clean package assembly:single
  2- Zips the solr directory and copy it to the directory target/oozie-workflow, this is later required by the map reduce process.
    mvn -P{environment profile},solr package assembly:single
    
    
 Running the workflow
 ---------------------
  The script install-workflow.sh builds, installs and rus the Oozie workflow, it requires two parameters:
  * A Maven profile name: dev, uat or prod.
  * A Git authorization token used to download the environment configuration from the repository https://github.com/gbif/gbif-configuration
    
     
 Conventions
 -----------
  * Variables follow the camelcase format, for example:solrCollection
  * Scripts  are named with words separated by character '_', for example: prepare_index.sh
  * Variables coming from the job configuration are named with words separated by '.', for example: solr.home
