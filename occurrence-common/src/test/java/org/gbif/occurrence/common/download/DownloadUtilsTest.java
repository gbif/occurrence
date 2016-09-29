package org.gbif.occurrence.common.download;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

/**
 * Simple test for DownloadUtils.
 */
public class DownloadUtilsTest {

  private char NUL_CHAR = '\0';

  @Test
  public void testNUllChar(){
    String testStr = "test";
    assertEquals(testStr, DownloadUtils.DELIMETERS_MATCH_PATTERN.matcher(testStr + NUL_CHAR).replaceAll(""));
  }
}
