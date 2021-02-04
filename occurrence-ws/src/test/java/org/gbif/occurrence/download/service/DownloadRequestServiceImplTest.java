package org.gbif.occurrence.download.service;

import org.apache.oozie.client.Job;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class DownloadRequestServiceImplTest {

  @Test
  public void testStatusMapCompleteness() throws Exception {
    for (Job.Status st : Job.Status.values()) {
      assertTrue(DownloadRequestServiceImpl.STATUSES_MAP.containsKey(st));
    }
  }
}
