package org.gbif.occurrence.common.download;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Simple test for DownloadUtils.
 */
public class DownloadUtilsTest {

  private static final char NUL_CHAR = '\0';

  @Test
  public void testNUllChar(){
    String testStr = "test";
    Assertions.assertEquals(testStr, DownloadUtils.DELIMETERS_MATCH_PATTERN.matcher(testStr + NUL_CHAR).replaceAll(""));
  }
}
