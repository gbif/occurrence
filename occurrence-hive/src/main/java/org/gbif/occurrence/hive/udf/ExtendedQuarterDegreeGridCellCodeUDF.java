package org.gbif.occurrence.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.gbif.occurrence.cube.functions.ExtendedQuarterDegreeGridCellCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Randomize a point according to its coordinateUncertainty (or some other distance), and determine the
 * Extended Quarter Degree Grid Cell in which the randomized point lies.
 */
@Description(name = "qdgcCode",
  value = "_FUNC_(Integer, Double, Double, Double) - description",
  extended = "Example: qdgcCode(1, decimalLatitude, decimalLongitude, COALESCE(coordinateUncertaintyInMeters, 1000))")
public class ExtendedQuarterDegreeGridCellCodeUDF extends UDF {

  private static final Logger LOG = LoggerFactory.getLogger(ExtendedQuarterDegreeGridCellCodeUDF.class.getName());

  private final ExtendedQuarterDegreeGridCellCode extendedQuarterDegreeGridCellCode = new ExtendedQuarterDegreeGridCellCode();

  private final Text resultString = new Text();

  public Text evaluate(IntWritable level, Double lat, Double lon, Double coordinateUncertaintyInMeters) {
    if (level == null || lat == null || lon == null || coordinateUncertaintyInMeters == null) {
      return null;
    }

    try {
      resultString.set(extendedQuarterDegreeGridCellCode.fromCoordinate(level.get(), lat, lon, coordinateUncertaintyInMeters));
      return resultString;
    } catch (Exception e) {
      return null;
    }
  }
}
