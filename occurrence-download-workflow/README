occurrence-download-workflow
----------------------------
This project contains the oozie workflow and hive queries needed to execute the occurrence download. The assembled
project needs to be copied to hdfs so that each of the task trackers can find the workflow definition and supporting
files. Jenkins is configured to do this. Otherwise use /scripts/deploy.sh to copy it over (please adapt local paths).

Example: mvn -Pdev clean package assembly:single

Installation
-------------
The workflow can be installed using the script bin/install.sh, this scripts builds the project
and copies the workflow into the HDFS, the parameters required by the script are the maven profile and  the workflow destination in the HDFS, for example:
./install.sh occurrence-download-workflow-dev /occurrence-download/dev/

Headers file "inc/0"
____________________
This file is used for big downloads only, it is copied into the hive folder of the temporary table created; the name  was intentionally chosen to be
the first file to be merged during a FileUtil.copyMerge operation.



