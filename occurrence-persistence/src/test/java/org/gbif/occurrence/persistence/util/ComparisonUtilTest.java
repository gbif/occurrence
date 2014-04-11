package org.gbif.occurrence.persistence.util;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ComparisonUtilTest {

  @Test
  public void testHashComparison() {
    String string1 = "I'm a string";
    String string2 = "I'm another string";
    assertTrue(ComparisonUtil.nullSafeEquals(string1, string1));
    assertFalse(ComparisonUtil.nullSafeEquals(string1, string2));
    assertFalse(ComparisonUtil.nullSafeEquals(null, string2));
    assertFalse(ComparisonUtil.nullSafeEquals(string1, null));
  }
}
