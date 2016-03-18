package org.gbif.occurrence.persistence.util;

import java.math.BigDecimal;
import java.net.URI;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

/**
 * Simple methods to do null safe comparisons.
 */
public class ComparisonUtil {

  private ComparisonUtil() {
  }

  public static boolean nullSafeEquals(String first, String second) {
    // both null or both the same not null string
    return first == null && second == null || first != null && first.equals(second);
  }

  public static boolean nullSafeEquals(UUID first, UUID second) {
    return first == null && second == null || first != null && first.equals(second);
  }

  public static boolean nullSafeEquals(Double first, Double second) {
    return first == null && second == null || first != null && first.equals(second);
  }

  public static boolean nullSafeEquals(Integer first, Integer second) {
    return first == null && second == null || first != null && first.equals(second);
  }

  public static boolean nullSafeEquals(Long first, Long second) {
    return first == null && second == null || first != null && first.equals(second);
  }

  public static boolean nullSafeEquals(byte[] first, byte[] second) {
    return first == null && second == null || first != null && Arrays.equals(first, second);
  }

  public static boolean nullSafeEquals(Enum first, Enum second) {
    return first == null && second == null || first != null && first == second;
  }

  public static boolean nullSafeEquals(Date first, Date second) {
    return first == null && second == null || first != null && second != null && first.getTime() == second.getTime();
  }

  public static boolean nullSafeEquals(URI first, URI second) {
    return first == null && second == null || first != null && second != null && first.equals(second);
  }

  public static boolean nullSafeEquals(BigDecimal first, BigDecimal second) {
    return first == null && second == null || first != null && second != null && first.equals(second);
  }
}
