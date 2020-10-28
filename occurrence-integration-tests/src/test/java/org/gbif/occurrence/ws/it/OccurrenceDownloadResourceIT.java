package org.gbif.occurrence.ws.it;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.ws.client.OccurrenceDownloadWsClient;
import org.gbif.ws.MethodNotAllowedException;
import org.gbif.ws.client.ClientFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.StreamUtils;

import static org.gbif.occurrence.ws.it.OccurrenceWsItConfiguration.TEST_USER;
import static org.gbif.occurrence.ws.it.OccurrenceWsItConfiguration.TEST_USER_PASSWORD;

@ExtendWith(SpringExtension.class)
@ActiveProfiles("test")
@AutoConfigureMockMvc
@SpringBootTest(
  classes = OccurrenceWsItConfiguration.class,
  webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class OccurrenceDownloadResourceIT {

  private static final String TEST_DOWNLOAD_FILE = "classpath:0011066-200127171203522.zip";

  private final OccurrenceDownloadWsClient downloadWsClient;

  private final OccurrenceDownloadService occurrenceDownloadService;

  private final ResourceLoader resourceLoader;

  private final int localServerPort;

  @Autowired
  public OccurrenceDownloadResourceIT(@LocalServerPort int localServerPort,
                                      OccurrenceDownloadService occurrenceDownloadService,
                                      ResourceLoader resourceLoader) {
    ClientFactory clientFactory = new ClientFactory(TEST_USER.getUserName(),
                                                    TEST_USER_PASSWORD,
                                                    "http://localhost:" + localServerPort);

    this.localServerPort = localServerPort;
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
  public void startDownloadWithDifferentUserAndCreatorTest() {
    PredicateDownloadRequest predicateDownloadRequest = testPredicateDownloadRequest();
    //Change creator user
    predicateDownloadRequest.setCreator("NotMe");

    //Exception expected
    Assertions.assertThrows(MethodNotAllowedException.class, () -> downloadWsClient.create(predicateDownloadRequest));
  }

  @Test
  public void startDownloadAuthenticationError() {
    ClientFactory clientFactory = new ClientFactory(TEST_USER.getUserName(),
                                                    "NotThePasword",
                                                    "http://localhost:" + localServerPort);
    DownloadRequestService downloadService = clientFactory.newInstance(OccurrenceDownloadWsClient.class);

    //Exception expected
    Assertions.assertThrows(AccessDeniedException.class, () -> downloadService.create(testPredicateDownloadRequest()));
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
    Assertions.assertArrayEquals(StreamUtils.copyToByteArray(resourceLoader.getResource(TEST_DOWNLOAD_FILE).getInputStream()),
                                 StreamUtils.copyToByteArray(downloadWsClient.getResult(downloadKey)),
                            "Content file of download file differs to expected content!");
  }


  @Test
  @SneakyThrows
  public void getStreamingDownloadResultTest() {
    Resource testFile = resourceLoader.getResource(TEST_DOWNLOAD_FILE);

    //Make 3 requests to get the entire content
    long chunkSize = testFile.getFile().length() / 3;


    //Create
    String downloadKey = downloadWsClient.create(testPredicateDownloadRequest());

    //Check is not null
    Assertions.assertNotNull(downloadKey, "DownloadKey is null!");

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    downloadWsClient.getStreamResult(downloadKey, chunkSize, chunk -> {
      try {
        byteArrayOutputStream.write(IOUtils.toByteArray(chunk));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    });

    //Is the content what it was expected
    Assertions.assertArrayEquals(StreamUtils.copyToByteArray(resourceLoader.getResource(TEST_DOWNLOAD_FILE).getInputStream()),
                                 byteArrayOutputStream.toByteArray(),
                                 "Content file of download file differs to expected content!");
  }


}
