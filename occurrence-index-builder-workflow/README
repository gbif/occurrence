occurrence-index-builder-workflow
---------------------------------
This project contains the oozie workflow that builds and deploy and Occurrence Solr Index from scratch. The assembled
project needs to be copied to hdfs so that each of the task trackers can find the workflow definition and supporting
files. 

To run the oozie workflow the following variable must be set in the configuration file:
  * sourceOccurrenceTable: name of the source hive table that contains the occurrence data that will be indexed.  
  * solrCollection: name of the SolrCloud collection, this collection will be  completely deleted before the index is created.
  * solrCloudDistOpts: options that determine the how the Solr index is distributed.
                          For more information please read the solrctl command documentation: http://www.cloudera.com/content/cloudera-content/cloudera-docs/Search/latest/Cloudera-Search-User-Guide/csug_solrctl_ref.html.
                          the command that uses this parameter is "solrctl collection --create solr_collection solr_cloud_dist_opts2"
  
  
The project is built following 2 steps:
  1- Create the workflow folder
    mvn -Poccurrence-index-builder-workflow,oozie clean package assembly:single
  2- Zips the solr directory and copy it to the directory target/oozie-workflow, this is later required by the map reduce process.
    mvn -Poccurrence-index-builder-workflow,solr package assembly:single
    
    
 Running the workflow
 ---------------------
  The bin directory contains 2 files
  * run.sh: script that runs the indexing process that creates a SolrCloud index, the following steps are executed:
        1- compiles the project and creates a directory target/oozie-workflow/ that contains all the files required to run the workflow
        2- clears the directory where the workflow will be copied
        3- copies the workflow files into the target directory in HDFS
        4- runs the workflow
  * job.properties: contains all the required settings to run the process:  
     * nameNode: hadoop name node
     * jobTracker: hadoop job tracker
     * queueName mapreduce queue name
     * oozieWfDestination= HDFS directory where the workflow will be copied     
     * sourceOccurrenceTable: hive occurrence table, please see source_occurrence_table above
     * hiveDB: hive database where the sourceOccurrenceTable is located, use 'default' if the table is not a specific database
     * solrCollection: name of the solr collection name, please see solrCollection above
     * solrCloudDistOpts: distribution options for the solrctl, please see solrCloudDistOpts above
     * zkHost: SolrCloud zookeeper host/ensemble
     Required by oozie cli client:
     -----------------------------
     * oozie.wf.application.path: path to the oozie workflow, this property must not be modified.
     * oozie.server: oozie server url
     
  * runSingleShardIndexer.sh: runs the indexing process that creates a single Solr index (1 shard),  the following steps are executed:
        1- compiles the project and creates a directory target/oozie-workflow/ that contains all the files required to run the workflow
        2- clears the directory where the workflow will be copied
        3- copies the workflow files into the target directory in HDFS
        4- runs the workflow
        
  Creating the SolrCloud index
  ----------------------------
  This process involves some manual steps  prior to execute the "run.sh" script:
    1. Logon into one of the Hadoop cluster machines using ssh: ssh root@c4n1.gbif.org.
    2. Delete the Solr collections and instancedirs that will installed if those exist already:
       solrctl collection --delete prod_occurrence
       solrctl instancedir --delete prod_occurrence
    3. Go to the Cloudera Manager console and STOP all the Solr instances (the next step requires that all the Solr instances are down in order to clean the Zookeeper configuration.).
    4. From the cluster machine, clear the Solr configuration by executing the command:
       solrctl init --force       
    5. Go to the Cloudera Manager console and START all the Solr instances.
    6. Verify that all the values in the job.properties file are correct. 
    7. From your own machine, run the script "run.sh".
    
  
  Creating a single sharded Solr index
  ---------------------------------------
    1. Verify that the jobsingleshard.properties contains the right values.    
    2. IF the HDFS directory  ${nameNode}/solr/${solrCollection}_single/ exists, remove all the contents of the HDFS directory ${nameNode}/solr/${solrCollection}_single/
        hadoop fs -rm -r -skipTrash ${nameNode}/solr/${solrCollection}_single/*
       ELSE create the directory
        hadoop fs -mkdir ${nameNode}/solr/${solrCollection}_single/*
        
        Note: replace the variables "${variable}" using the values in file jobsingleshard.properties to get the desired values.
    3. Run the script "runSingleShardIndexer.sh".
    4. Once the script has finished, the index will be located in ${nameNode}/solr/${solrCollection}_single/results/part-00000/.
    
      Installing the new index on a machine running Solr with Jetty
      --------------------------------------------------------------
      1. From one of the Hadoop cluster machines, copy the Solr index from HDFS into the local file system:
        hadoop fs -copyToLocal -ignoreCrc ${nameNode}/solr/${solrCollection}_single/results/part-00000/* .        
      2. Copy the data directory to a temporary directory in the target server: 
        scp -r data/ root@server://var/local/large/solr/occurrence-solrtemp/
        rm -rf data/
      3. Logon into the machine that hosts the index and go to the directory where Jetty is running:
        cd //usr/local/jetty-occurrence/
      4. Stop Jetty:
        ./stop-jetty.sh
      5. Create a backup of the directory //mnt/ssd/solr/occurrence-solr/occurrence/data (//var/local/large/occurrence-solrbk/ has been the usual location for storing backups).
      6. Replace the content of directory //mnt/ssd/solr/occurrence-solr/occurrence/data with content of the data directory copied in //var/local/large/solr/occurrence-solrtemp
        rm -rf  //mnt/ssd/solr/occurrence-solr/occurrence/data/
        mv -f  //var/local/large/solr/occurrence-solrtemp/data/ //mnt/ssd/solr/occurrence-solr/occurrence/
      7.Start Jetty:
        ./start-jetty.sh
        
     Troubleshooting:
     ----------------
     * Sometime the script ./stop-jetty.sh doesn't stop Jetty properly, therefore, the Jetty process must be killed using the "kill -9" commnand.
    
     
 Conventions
 -----------
  * Variables follow the camelcase format, for example:solrCollection
  * Scripts  are named with words separated by character '_', for example: prepare_index.sh
