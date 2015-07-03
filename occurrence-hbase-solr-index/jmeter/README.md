Occurrence Solr -JMeter
=======================
This directory contains a JMeter project to execute performance tests on Solr faceted searches for GBIF Occurrence Solr Index.

Requirements
------------
  * apache-jmeter-2.13
  * java > x.6
  * JMeter [plugins](http://jmeter-plugins.org/): install the [StandardSet](http://jmeter-plugins.org/wiki/StandardSet/) and [ExtrasSet](http://jmeter-plugins.org/wiki/ExtrasSet/)   
    - http://jmeter-plugins.org/wiki/JMXMon/
    - http://jmeter-plugins.org/wiki/VariablesFromCSV/    

Files
------
  * [jmeter.properties](jmeter.properties){:target="_blank"}: it is a straight copy of the jmeter.properties, it has been copied to facilitate the execution of JMeter from this directory.
  * [user.properties](user.properties){:target="_blank"}: contains additional JMeter properties, Solr configuration settings are obtained from this file:
    - solr_server: Solr server name
    
    - solr_port: Solr server port (default value: 8983)
    
    - solr_collection: SolrCloud collection/core
    
    - solr_jmx_port: Solr JMX port (default value: 3000)
    
      Note: every time that you change this file JMeter has to be restarts, i.e.: JMeter loads it at startup time
      
      
  * [solr_parameters.csv](solr_parameters.csv): CSV file with the Solr query parameters to be used, its expected format is "q,facet,fq1,fq2"
  
    - q: Solr [main query](https://wiki.apache.org/solr/CommonQueryParameters#q)
    
    - facet: Solr [facet field](https://wiki.apache.org/solr/SimpleFacetParameters#facet.field).
    
      See the Occurrence Solr [schema](../src/main/resources/solr/conf/schema.xml) for possible values for this parameter.
    - fq1: Solr [filter query](https://wiki.apache.org/solr/CommonQueryParameters#fq) 1
    
    - fq2: Solr [filter query](https://wiki.apache.org/solr/CommonQueryParameters#fq) 2, only 2 filter queries are accepted by this JMeter test plan.
    
      Note: by default the following values are used: facet=true, facet.limit=-1, facet.mincount=1 and rows=0.
      
Execution
---------
From this directory, and assuming that JMeter is in the OS path, run:

    - `jmeter -t OccurrenceFacetsTests.jmx` to the test file in the JMeter GUI.
    - `jmeter -n -t OccurrenceFacetsTests.jmx` to run test in Non-GUI mode.
