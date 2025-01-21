package org.gbif.occurrence.download.sql;

import org.apache.spark.sql.SparkSession;

public interface SparkSessionSupplier {
    SparkSession create();
}
