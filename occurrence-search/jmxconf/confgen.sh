while read line; do 
  IFS=' ' read -a server <<< "$line"
  solrserver=${server[0]}
  jmxport=${server[1]}
  solrcore=${server[2]}
  groupalias=${server[3]}
  groupname=${server[4]}
  gmondserver=${server[5]}
  gmondport=${server[6]}  
  outputfile=$2$solrserver.$solrcore.xml 
  sed -e "s/\${solrserver}/$solrserver/" -e "s/\${jmxport}/$jmxport/" -e "s/\${solrcore}/$solrcore/" -e "s/\${groupalias}/$groupalias/"  -e "s/\${groupname}/$groupname/" -e "s/\${gmondserver}/$gmondserver/" -e "s/\${gmondport}/$gmondport/" $1 > $outputfile  
done < servers.properties
