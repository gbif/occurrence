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
package org.gbif.metrics.ws.client;

import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.util.SortedMap;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for {@link OccurrenceDatasetIndexWsClient} against a stubbed server (WireMock).
 * Verifies the client contract matches occurrence-search-ws REST API.
 */
class OccurrenceDatasetIndexWsClientIT {

  private static final String DATASET_KEY = "d596fccb-2319-42eb-b13b-986c932780ad";

  private WireMockServer wireMock;
  private OccurrenceDatasetIndexWsClient client;

  @BeforeEach
  void init() {
    wireMock = new WireMockServer(WireMockConfiguration.options().dynamicPort());
    wireMock.start();
    client =
        new ClientBuilder()
            .withUrl("http://localhost:" + wireMock.port())
            .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
            .build(OccurrenceDatasetIndexWsClient.class);
  }

  @AfterEach
  void tearDown() {
    if (wireMock != null) {
      wireMock.stop();
    }
  }

  @Test
  void occurrenceDatasetsForCountry() {
    wireMock.stubFor(
        get(urlPathEqualTo("/occurrence/counts/datasets"))
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"" + DATASET_KEY + "\": 100}")));

    SortedMap<UUID, Long> result = client.occurrenceDatasetsForCountry("DE");
    assertNotNull(result);
    assertEquals(1, result.size());
    assertEquals(100L, result.get(UUID.fromString(DATASET_KEY)));
  }

  @Test
  void occurrenceDatasetsForNubKey() {
    wireMock.stubFor(
        get(urlPathEqualTo("/occurrence/counts/datasets"))
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withBody("{}")));

    SortedMap<UUID, Long> result = client.occurrenceDatasetsForNubKey(212);
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }
}
