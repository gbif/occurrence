package org.gbif.occurrence.download.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import org.gbif.api.model.common.DOI;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.occurrence.query.TitleLookupService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

public class DownloadEmailUtilsTest {

  @Test
  public void testSendSuccessNotificationMail() throws Exception {
    TitleLookupService tl = mock(TitleLookupService.class);
    when(tl.getDatasetTitle(ArgumentMatchers.any())).thenReturn("The little Mermaid");
    when(tl.getSpeciesName(ArgumentMatchers.any())).thenReturn("Abies alba Mill.");

    DownloadEmailUtils utils = new DownloadEmailUtils("1@mailinator.com; 2@mailinator.com", "https://www.gbif.org", null, null, tl);
    Download d = new Download();
    d.setKey("0007082-141215154445624");
    d.setDoi(new DOI("10.5072/dl.j9spoa"));
    d.setDownloadLink("http://api.gbif.org/v1/occurrence/download/request/0007082-141215154445624.zip");
    d.setCreated(new Date());
    d.setStatus(Download.Status.SUCCEEDED);
    d.setModified(new Date());
    d.setNumberDatasets(3);
    d.setSize(1787823);
    d.setTotalRecords(8792);
    d.setEraseAfter(Date.from(OffsetDateTime.now(ZoneOffset.UTC).plusMonths(6).toInstant()));

    PredicateDownloadRequest req = new PredicateDownloadRequest();
    req.setFormat(DownloadFormat.SIMPLE_CSV);
    req.setCreator("markus");
    d.setRequest(req);
    String body = utils.buildBody(d, "success.ftl");

    Assertions.assertNotNull(body);

    d.setStatus(Download.Status.FAILED);
    body = utils.buildBody(d, "error.ftl");
    Assertions.assertNotNull(body);
  }
}
