package org.gbif.occurrence.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.gbif.occurrence.cube.functions.Isea3hCellCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Randomize a point according to its coordinateUncertainty (or some other distance), and determine the
 * ISEA3H Cell in which the randomized point lies at the given resolution.
 */
@Description(name = "isea3hCellCode",
  value = "_FUNC_(Integer, Double, Double, Double) - description",
  extended = "Example: isea3hCellCode(6, decimalLatitude, decimalLongitude, COALESCE(coordinateUncertaintyInMeters, 1000))")
public class Isea3hCellCodeUDF extends UDF {

  private static final Logger LOG = LoggerFactory.getLogger(Isea3hCellCodeUDF.class.getName());

  private final Isea3hCellCode isea3hCellCode = new Isea3hCellCode();

  public Text evaluate(IntWritable resolution, Double lat, Double lon, Double coordinateUncertaintyInMeters) {
    if (resolution == null || lat == null || lon == null || coordinateUncertaintyInMeters == null) {
      return null;
    }

    final Text resultString = new Text();

    try {
      resultString.set(isea3hCellCode.fromCoordinate(resolution.get(), lat, lon, coordinateUncertaintyInMeters));
      return resultString;
    } catch (Exception e) {
      return null;
    }
  }
}
