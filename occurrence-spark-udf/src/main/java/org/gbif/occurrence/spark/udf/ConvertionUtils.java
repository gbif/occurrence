package org.gbif.occurrence.spark.udf;

/**
 * Utility class for converting values.
 */
public class ConvertionUtils {

  /**
   * Converts a Number to a Double.
   */
  public static Double toDouble(Number value) {
    if (value == null) {
      return null;
    }
    return value.doubleValue();
  }
}
