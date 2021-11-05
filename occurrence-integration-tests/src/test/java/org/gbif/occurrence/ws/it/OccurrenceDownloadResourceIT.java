/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.ws.it;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.ws.client.OccurrenceDownloadWsClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.core.io.ResourceLoader;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.fasterxml.jackson.core.JsonProcessingException;

import lombok.SneakyThrows;

import static org.gbif.occurrence.ws.it.OccurrenceWsItConfiguration.TEST_USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
  public OccurrenceDownloadResourceIT(
      @LocalServerPort int localServerPort,
      OccurrenceDownloadService occurrenceDownloadService,
      ResourceLoader resourceLoader) {
    ClientBuilder clientBuilder =
        new ClientBuilder()
            .withUrl("http://localhost:" + localServerPort)
            .withCredentials(
                TEST_USER.getUserName(),
                TEST_USER
                    .getUserName()) // actually no needed since the remote auth client is mocked
            .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
            .withFormEncoder();

    this.localServerPort = localServerPort;
    this.downloadWsClient = clientBuilder.build(OccurrenceDownloadWsClient.class);
    this.occurrenceDownloadService = occurrenceDownloadService;
    this.resourceLoader = resourceLoader;
  }

  /** Creates a test entity with a download all request. */
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
    assertNotNull(downloadKey, "DownloadKey is null!");
  }

  @Test
  public void startDownloadWithDifferentUserAndCreatorTest() throws JsonProcessingException {
    PredicateDownloadRequest predicateDownloadRequest = testPredicateDownloadRequest();
    // Change creator user
    predicateDownloadRequest.setCreator("NotMe");

    // Exception expected
    assertThrows(
        AccessDeniedException.class, () -> downloadWsClient.create(predicateDownloadRequest));
  }

  @Test
  public void cancelDownloadTest() {
    // Create
    String downloadKey = downloadWsClient.create(testPredicateDownloadRequest());
    assertNotNull(downloadKey, "DownloadKey is null!");

    // Cancel
    downloadWsClient.cancel(downloadKey);

    // Check
    Download download = occurrenceDownloadService.get(downloadKey);
    assertNotNull(download, "Cancelled download is null!");
    assertEquals(
        Download.Status.CANCELLED,
        download.getStatus(),
        "Occurrence download status is not Cancelled!");
  }

  @Test
  @SneakyThrows
  public void getDownloadResultTest() {
    // Create
    String downloadKey = downloadWsClient.create(testPredicateDownloadRequest());

    // Check is not null
    assertNotNull(downloadKey, "DownloadKey is null!");

    // Is the content what it was expected
    assertEquals(302, downloadWsClient.getDownloadResult(downloadKey, null).status());
  }
}
