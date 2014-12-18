package org.gbif.occurrence.download.service;

import org.gbif.api.model.common.DOI;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadRequest;

import java.util.Date;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class DownloadEmailUtilsTest {

  @Test
  public void testSendSuccessNotificationMail() throws Exception {
    DownloadEmailUtils utils = new DownloadEmailUtils("1@mailinator.com, 2@mailinator.com", "http:///www.gbif.org", null, null, null, null);
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
    DownloadRequest req = new DownloadRequest();
    req.setCreator("markus");
    d.setRequest(req);
    String body = utils.buildBody(d, "success.ftl");
    //System.out.println(body);
    assertNotNull(body);


    d.setStatus(Download.Status.FAILED);
    body = utils.buildBody(d, "error.ftl");
    //System.out.println(body);
    assertNotNull(body);
  }
}