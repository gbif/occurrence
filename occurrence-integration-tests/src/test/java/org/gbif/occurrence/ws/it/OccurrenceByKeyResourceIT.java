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

import java.util.UUID;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.occurrence.test.extensions.ElasticsearchInitializer;
import org.gbif.occurrence.ws.client.OccurrenceWsClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(SpringExtension.class)
@ActiveProfiles("test")
@AutoConfigureMockMvc
@SpringBootTest(
    classes = {
      org.gbif.metrics.ws.OccurrenceSearchWsApplication.class,
      OccurrenceSearchWsItConfiguration.class
    },
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OccurrenceByKeyResourceIT {

  private static final String TEST_DATA_FILE = "classpath:occurrences-test.json";
  private static final Long TEST_KEY = 648006L;

  @RegisterExtension
  static ElasticsearchInitializer elasticsearchInitializer =
      ElasticsearchInitializer.builder().testDataFile(TEST_DATA_FILE).build();

  private final OccurrenceWsClient occurrenceWsClient;

  @Autowired
  public OccurrenceByKeyResourceIT(@LocalServerPort int localServerPort) {
    ClientBuilder clientBuilder =
        new ClientBuilder()
            .withUrl("http://localhost:" + localServerPort)
            .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
            .withFormEncoder();
    occurrenceWsClient = clientBuilder.build(OccurrenceWsClient.class);
  }

  @Test
  void testGetByKey() {
    Occurrence occurrence = occurrenceWsClient.get(TEST_KEY);
    assertNotNull(occurrence, "Empty occurrence received");
  }

  @Test
  void testGetByDatasetKeyAndOccurrenceId() {
    Occurrence occurrence =
        occurrenceWsClient.get(
            UUID.fromString("d596fccb-2319-42eb-b13b-986c932780ad"), "MGYA00167231_Bacteria");
    assertNotNull(occurrence, "Empty occurrence received");
  }

  @Test
  void testGetVerbatim() {
    VerbatimOccurrence verbatim = occurrenceWsClient.getVerbatim(TEST_KEY);
    assertNotNull(verbatim, "Empty verbatim record!");
  }
}
