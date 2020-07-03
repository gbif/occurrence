package org.gbif.occurrence.it.ws;

import org.gbif.api.model.common.GbifUserPrincipal;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.ws.client.OccurrenceDownloadWsClient;
import org.gbif.ws.client.ClientFactory;

import java.security.Principal;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.core.io.ResourceLoader;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.StreamUtils;

import static org.gbif.occurrence.it.ws.OccurrenceWsItConfiguration.TEST_USER;

@ExtendWith(SpringExtension.class)
@ActiveProfiles("test")
@AutoConfigureMockMvc
@SpringBootTest(
  classes = OccurrenceWsItConfiguration.class,
  webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class OccurrenceDownloadResourceIT {

  private static final String TEST_DOWNLOAD_FILE = "classpath:0011066-200127171203522.zip";

  private static final Principal TEST_PRINCIPAL = new GbifUserPrincipal(TEST_USER);

  private final DownloadRequestService downloadWsClient;

  private final OccurrenceDownloadService occurrenceDownloadService;

  private final ResourceLoader resourceLoader;


  @Autowired
  public OccurrenceDownloadResourceIT(@LocalServerPort int localServerPort,
                                      OccurrenceDownloadService occurrenceDownloadService,
                                      ResourceLoader resourceLoader) {
    ClientFactory clientFactory = new ClientFactory(TEST_USER.getUserName(),
                                                    TEST_USER.getPasswordHash(),
                                                    "http://localhost:" + localServerPort);
    this.downloadWsClient = clientFactory.newInstance(OccurrenceDownloadWsClient.class);
    this.occurrenceDownloadService = occurrenceDownloadService;
    this.resourceLoader = resourceLoader;
  }

  /**
   * Creates a test entity with a download all request.
   */
  private PredicateDownloadRequest testPredicateDownloadRequest() {
    PredicateDownloadRequest predicateDownloadRequest = new PredicateDownloadRequest();
    predicateDownloadRequest.setFormat(DownloadFormat.SIMPLE_CSV);
    predicateDownloadRequest.setCreator(TEST_USER.getUserName());
    predicateDownloadRequest.setNotificationAddressesAsString(TEST_USER.getEmail());
    return predicateDownloadRequest;
  }

  @Test
  public void startDownloadTest() {
    String downloadKey = downloadWsClient.create(testPredicateDownloadRequest());
    Assertions.assertNotNull(downloadKey, "DownloadKey is null!");
  }


  @Test
  public void cancelDownloadTest() {
    //Create
    String downloadKey = downloadWsClient.create(testPredicateDownloadRequest());
    Assertions.assertNotNull(downloadKey, "DownloadKey is null!");

    //Cancel
    downloadWsClient.cancel(downloadKey);

    //Check
    Download download = occurrenceDownloadService.get(downloadKey);
    Assertions.assertNotNull(download, "Cancelled download is null!");
    Assertions.assertEquals(Download.Status.CANCELLED, download.getStatus(), "Occurrence download status is not Cancelled!");
  }


  @Test
  @SneakyThrows
  public void getDownloadResultTest() {
    //Create
    String downloadKey = downloadWsClient.create(testPredicateDownloadRequest());

    //Check is not null
    Assertions.assertNotNull(downloadKey, "DownloadKey is null!");

    //Is the content what it was expected
    Assertions.assertEquals(StreamUtils.copyToByteArray(resourceLoader.getResource(TEST_DOWNLOAD_FILE).getInputStream()),
                            StreamUtils.copyToByteArray(downloadWsClient.getResult(downloadKey)),
                            "Content file of download file differs to expected content!");
  }


}
