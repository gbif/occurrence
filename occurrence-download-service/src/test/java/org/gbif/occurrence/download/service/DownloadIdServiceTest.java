package org.gbif.occurrence.download.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DownloadIdServiceTest {

  @Test
  public void testId() {
    DownloadIdService downloadIdService = new DownloadIdService();

    //The first id is padded with 7 0s
    String id = downloadIdService.generateId();
    assertTrue(id.startsWith("0000000"));

    //The second id is 1 padded with 6 0s and so on
    String id2 = downloadIdService.generateId();
    assertTrue(id2.startsWith("0000001"));

    //Both ids use the same post-fix which is the start time
    assertEquals(id.substring(8), id2.substring(8));
  }
}
