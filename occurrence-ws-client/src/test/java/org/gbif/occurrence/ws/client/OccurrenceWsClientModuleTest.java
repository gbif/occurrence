package org.gbif.occurrence.ws.client;

import static org.junit.Assert.assertNotNull;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.gbif.ws.client.guice.AnonymousAuthModule;
import org.gbif.ws.client.guice.SingleUserAuthModule;
import org.junit.Ignore;
import org.junit.Test;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

public class OccurrenceWsClientModuleTest {

  @Test
  public void testModule() throws IOException, URISyntaxException {
    Properties properties = PropertiesUtil.loadProperties("occurrence-test.properties");
    OccurrenceWsClientModule mod = new OccurrenceWsClientModule(properties);
    Module auth = new AnonymousAuthModule();

    Injector inj = Guice.createInjector(auth, mod);
    assertNotNull(inj.getInstance(OccurrenceService.class));
  }


  @Test
  @Ignore("manual test class to verify a local download webservice")
  public void testCreate() throws Exception {
    final String USER = "nagios";

    Properties props = new Properties();
    // props.setProperty("occurrencedownload.ws.url", "http://apidev.gbif.org/");
    // props.setProperty("occurrencedownload.ws.url", "http://localhost:8080/");
    // props.setProperty("occurrencedownload.ws.url", "http://jawa.gbif.org:8080/occurrence-download-ws/");
    props.setProperty("occurrencedownload.ws.url", "http://localhost:8080/occurrence-download-ws/");
    OccurrenceWsClientModule mod = new OccurrenceWsClientModule(props);
    SingleUserAuthModule authMod = new SingleUserAuthModule(USER, USER);
    Injector inj = Guice.createInjector(authMod, mod);

    DownloadRequestService client = inj.getInstance(DownloadRequestService.class);

    DownloadRequest d =
      new PredicateDownloadRequest(new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "5219426"), USER, null, true,
                          DownloadFormat.DWCA);

    client.create(d);

    System.out.print(d);
  }


  private void f(){
    Properties props = new Properties();
    props.setProperty("occurrence.ws.url", "api.gbif.org/v1/");
    Injector inj = Guice.createInjector(new OccurrenceWsClientModule(props));
    OccurrenceService occService = inj.getInstance(OccurrenceService.class);
    OccurrenceSearchService searchService = inj.getInstance(OccurrenceSearchService.class);
  }

}
