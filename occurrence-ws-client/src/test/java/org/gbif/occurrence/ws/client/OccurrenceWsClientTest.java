package org.gbif.occurrence.ws.client;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.service.occurrence.DownloadRequestService;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class OccurrenceWsClientTest {


  private DownloadRequestService downloadRequestService;

  public OccurrenceWsClientTest(DownloadRequestService downloadRequestService) {
    this.downloadRequestService = downloadRequestService;
  }


  @Test
  @Ignore("manual test class to verify a local download webservice")
  public void testCreate() throws Exception {
    final String USER = "nagios";

    DownloadRequest d =
      new PredicateDownloadRequest(new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "5219426"),
                                   "nagios",null, true, DownloadFormat.DWCA);

    String key = downloadRequestService.create(d);
    Assert.assertNotNull(key);
  }

}
