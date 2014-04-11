occurrence-tables-coord
----------------------------
This project contains the oozie workflow and coordinates needed to build the occurrence HDFS tables. The assembled
project needs to be copied to hdfs so that each of the task trackers can find the workflow definition and supporting
files. To build the oozie-workflow using maven use the following command (the example uses the 'dev' profile):
  mvn -Pdev clean package assembly:single
The previous command generates a directory target/oozie-workflow that contains the oozie workflow and the coordinator.xml file.

The following properties are required in a maven profile:
   - occurrence.environment
   - hadoop.jobtracker
   - hdfs.namenode
   - zookeeper.quorum
   - hive.metastore.uris
   - occurrence.download.hive.hdfs.out


Installation
-------------
The workflow can be installed using the script bin/install.sh, this scripts builds the project
and copies the workflow into the HDFS, the parameters required by the script are the maven profile,  the workflow destination in the HDFS and  the oozie url, for example:
 ./install.sh dev dev http://c1n8.gbif.org:11000/oozie/
will install the workflow  in the HDFS directory occurrence-tables-coord/dev using the 'dev' profile, and will run the coordinator job using the ooze server http://c1n8.gbif.org:11000/oozie/

Note: the installation scripts uses the default directory 'occurrence-tables-coord'.


