#!/usr/bin/env bash

######################################################################
#
# Running this script will set two environment variables:
# HADOOP_CLASSPATH
# HADOOP_LIBJAR: pass this to the -libjar MapReduceIndexBuilder option
#
######################################################################

# return absolute path
function absPath {
  echo $(cd $(dirname "$1"); pwd)/$(basename "$1")
}


# Find location of this script

sdir="`cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd`"

solr_distrib="$sdir/../../.."

echo `absPath $solr_distrib`

# Setup env variables for MapReduceIndexerTool

# Setup HADOOP_CLASSPATH

dir1=`absPath "$solr_distrib/dist"`
dir2=`absPath "$solr_distrib/dist/solrj-lib"`
dir3=`absPath "$solr_distrib/contrib/map-reduce/lib"`
dir4=`absPath "$solr_distrib/contrib/morphlines-core/lib"`
dir5=`absPath "$solr_distrib/contrib/morphlines-cell/lib"`
dir6=`absPath "$solr_distrib/contrib/extraction/lib"`
dir7=`absPath "$solr_distrib/server/solr-webapp/webapp/WEB-INF/lib"`

# Setup -libjar

lib1=`ls $dir1/*.jar | awk '{print "file://"$1","}' | tr -d ' \n' | sed 's/.$//'`
lib2=`ls $dir2/*.jar | awk '{print "file://"$1","}' | tr -d ' \n' | sed 's/.$//' | sed 's/\,[^\,]*\(log4j\|slf4j\)[^\,]*//g'`
lib3=`ls $dir3/*.jar | awk '{print "file://"$1","}' | tr -d ' \n' | sed 's/.$//' | sed 's/\,[^\,]*\(hadoop\)[^\,]*//g'`
lib4=`ls $dir4/*.jar | awk '{print "file://"$1","}' | tr -d ' \n' | sed 's/.$//'`
lib5=`ls $dir5/*.jar | awk '{print "file://"$1","}' | tr -d ' \n' | sed 's/.$//'`
lib6=`ls $dir6/*.jar | awk '{print "file://"$1","}' | tr -d ' \n' | sed 's/.$//'`
lib7=`ls $dir7/*.jar | awk '{print "file://"$1","}' | tr -d ' \n' | sed 's/.$//' | sed 's/\,[^\,]*\(hadoop\)[^\,]*//g'`

export HADOOP_CLASSPATH="$dir1/*:$dir2/*:$dir3/*:$dir4/*:$dir5/*:$dir6/*:$dir7/*"
export HADOOP_LIBJAR="$lib1,$lib2,$lib3,$lib4,$lib5,$lib6,$lib7"

#echo $HADOOP_CLASSPATH
#echo $HADOOP_LIBJAR

