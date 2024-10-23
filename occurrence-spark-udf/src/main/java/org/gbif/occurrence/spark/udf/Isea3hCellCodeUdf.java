package org.gbif.occurrence.spark.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.sql.api.java.UDF4;
import org.gbif.occurrence.cube.functions.ExtendedQuarterDegreeGridCellCode;
import org.gbif.occurrence.cube.functions.Isea3hCellCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Randomize a point according to its coordinateUncertainty (or some other distance), and determine the
 * ISEA3H Cell in which the randomized point lies at the given resolution.
 */
public class Isea3hCellCodeUdf implements UDF4<Integer,Double,Double,Double,String> {

  private final Isea3hCellCode isea3hCellCode = new Isea3hCellCode();

  @Override
  public String call(Integer level, Double lat, Double lon, Double coordinateUncertaintyInMeters) throws Exception {
    return isea3hCellCode.fromCoordinate(level, lat, lon, coordinateUncertaintyInMeters);
  }
}
