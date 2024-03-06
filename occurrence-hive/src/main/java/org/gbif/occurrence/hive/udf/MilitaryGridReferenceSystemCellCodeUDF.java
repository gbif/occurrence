package org.gbif.occurrence.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.gbif.occurrence.cube.functions.MilitaryGridReferenceSystemCellCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Randomize a point according to its coordinateUncertainty (or some other distance), and determine the
 * Military Grid Reference System Grid Cell in which the randomized point lies.
 */
@Description(name = "mgrsCellCode",
  value = "_FUNC_(Integer, Double, Double, Double) - description",
  extended = "Example: mgrsCellCode(1000, decimalLatitude, decimalLongitude, COALESCE(coordinateUncertaintyInMeters, 1000))")
public class MilitaryGridReferenceSystemCellCodeUDF extends UDF {

  private static final Logger LOG = LoggerFactory.getLogger(MilitaryGridReferenceSystemCellCodeUDF.class.getName());

  private final MilitaryGridReferenceSystemCellCode mgrsCellCode = new MilitaryGridReferenceSystemCellCode();

  public Text evaluate(IntWritable gridSize, Double lat, Double lon, Double coordinateUncertaintyInMeters) {
    if (gridSize == null || lat == null || lon == null || coordinateUncertaintyInMeters == null) {
      return null;
    }

    final Text resultString = new Text();

    try {
      resultString.set(mgrsCellCode.fromCoordinate(gridSize.get(), lat, lon, coordinateUncertaintyInMeters));
      return resultString;
    } catch (Exception e) {
      return null;
    }
  }
}
