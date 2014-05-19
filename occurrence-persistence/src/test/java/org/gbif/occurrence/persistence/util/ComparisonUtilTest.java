package org.gbif.occurrence.persistence.util;

import java.net.URI;

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

  @Test
  public void testUriComparison() {
    URI uri1 = URI.create("http://www.gbif.org");
    URI uri2 = URI.create("http://www.gbif.org");
    URI uri3 = URI.create("http://www.gbif.org/dataset");
    assertTrue(ComparisonUtil.nullSafeEquals(uri1, uri1));
    assertTrue(ComparisonUtil.nullSafeEquals(uri1, uri2));
    assertFalse(ComparisonUtil.nullSafeEquals(uri1, uri3));
    assertFalse(ComparisonUtil.nullSafeEquals(null, uri1));
    assertFalse(ComparisonUtil.nullSafeEquals(uri2, null));
  }

}
