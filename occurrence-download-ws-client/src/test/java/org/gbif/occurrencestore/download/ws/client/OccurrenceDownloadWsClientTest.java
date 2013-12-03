package org.gbif.occurrencestore.download.ws.client;

import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.occurrencestore.download.ws.client.guice.OccurrenceDownloadWsClientModule;
import org.gbif.ws.client.guice.SingleUserAuthModule;

import java.util.Properties;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("manual test class to verify a local download webservice")
public class OccurrenceDownloadWsClientTest {

  @Test
  public void testCreate() throws Exception {
    final String USER = "markus";

    Properties props = new Properties();
    //props.setProperty("occurrencedownload.ws.url", "http://apidev.gbif.org/");
    //props.setProperty("occurrencedownload.ws.url", "http://localhost:8080/");
    // props.setProperty("occurrencedownload.ws.url", "http://jawa.gbif.org:8080/occurrence-download-ws/");
    props.setProperty("occurrencedownload.ws.url", "http://localhost:8080/occurrence-download-ws/");
    OccurrenceDownloadWsClientModule mod = new OccurrenceDownloadWsClientModule(props);
    SingleUserAuthModule authMod = new SingleUserAuthModule(USER, USER);
    Injector inj = Guice.createInjector(authMod, mod);

    DownloadRequestService client = inj.getInstance(DownloadRequestService.class);

    DownloadRequest d =
      new DownloadRequest(new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "5219426"), USER, null);

    client.create(d);

    System.out.print(d);
  }

}
