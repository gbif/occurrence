package org.gbif.occurrence.download.service;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Date;

import org.gbif.api.model.common.DOI;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.SqlDownloadRequest;
import org.gbif.api.model.occurrence.predicate.ConjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.DisjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.InPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.occurrence.query.TitleLookup;
import org.junit.Test;
import org.mockito.Matchers;

public class DownloadEmailUtilsTest {

  @Test
  public void testSendSuccessNotificationMail() throws Exception {
    TitleLookup tl = mock(TitleLookup.class);
    when(tl.getDatasetTitle(Matchers.<String>any())).thenReturn("The little Mermaid");
    when(tl.getSpeciesName(Matchers.<String>any())).thenReturn("Abies alba Mill.");

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
    req.setPredicate(new ConjunctionPredicate(Arrays.asList(
      new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "1"),
      new InPredicate(OccurrenceSearchParameter.COUNTRY, Arrays.asList("SE", "FI", "AX")),
      new DisjunctionPredicate(Arrays.asList(
        new EqualsPredicate(OccurrenceSearchParameter.BASIS_OF_RECORD, "HUMAN_OBSERVATION"),
        new EqualsPredicate(OccurrenceSearchParameter.BASIS_OF_RECORD, "MACHINE_OBSERVATION")
      ))
    )));

    req.setFormat(DownloadFormat.SIMPLE_CSV);
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
  
  @Test
  public void testSendSuccessNotificationMailForSQLDownload() throws Exception {
    TitleLookup tl = mock(TitleLookup.class);
    when(tl.getDatasetTitle(Matchers.<String>any())).thenReturn("The little Mermaid");
    when(tl.getSpeciesName(Matchers.<String>any())).thenReturn("Abies alba Mill.");

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

    SqlDownloadRequest req = new SqlDownloadRequest();
    req.setFormat(DownloadFormat.SQL);
    req.setSql("SELECT gbifid, countrycode, basisofrecord\n" + 
        "FROM occurrence_HDFS\n" + 
        "WHERE countrycode = 'CO'");
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
