occurrence-tables-coord
----------------------------
This project contains the oozie workflow and coordinates needed to build the occurrence HDFS tables. The assembled
project needs to be copied to hdfs so that each of the task trackers can find the workflow definition and supporting
files. To build the oozie-workflow using maven use the following command (the example uses the 'dev' profile):
  mvn -Pdev clean package assembly:single
The previous command generates a directory target/oozie-workflow that contains the oozie workflow and the coordinator.xml file.

The following properties are required in a maven profile:
   - occurrence.environment
   - occurrence.env_prefix
   - hadoop.jobtracker
   - hdfs.namenode
   - zookeeper.quorum
   - hive.metastore.uris
   - occurrence.download.hive.hdfs.out


Installation
-------------
The workflow can be installed using the script bin/install.sh, this scripts builds the project using profiles.xml from gbif-configuration in github,
and copies the workflow into the HDFS, the parameters required by the script are the maven profile,  the workflow destination in the HDFS and  the oozie url, for example:
 ./install.sh dev dev http://c1n1.gbif.org:11000/oozie/ 1234578901234
will install the workflow  in the HDFS directory occurrence-tables-coord/dev using the 'dev' profile, and will run the coordinator job using the oozie server http://c1n1.gbif.org:11000/oozie/.
The final param in the script is a github auth token. The installation scripts uses the default directory 'occurrence-tables-coord'.

NOTE: you must change the start date in the coordinator.xml to the day after you run the install script, so that we don't get a million historical builds (one per missed day) started by oozie
