package org.gbif.occurrence.download.spark;

import lombok.Builder;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;

@Builder
public class SimpleCsvDownload {

  private final SparkSession sparkSession;
  private final String queryFile;

  public void run() {
    //run Queries
    SparkSqlFileRunner.runSQLFile(queryFile, new HashMap<>(), sparkSession);


    //citations

    //zip content



    //delete tables

  }
}
