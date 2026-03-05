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

import org.gbif.api.model.metrics.cube.Rollup;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Integration test for {@link CubeWsClient} against a stubbed server (WireMock).
 * Verifies the client contract matches occurrence-search-ws REST API.
 */
class CubeWsClientIT {

  private static final String SCHEMA_RESPONSE =
      "[{\"dimensions\":[{\"key\":\"basisOfRecord\",\"title\":\"Basis of record\"}]}]";

  private WireMockServer wireMock;
  private CubeWsClient client;

  @BeforeEach
  void init() {
    wireMock = new WireMockServer(WireMockConfiguration.options().dynamicPort());
    wireMock.start();
    client =
        new ClientBuilder()
            .withUrl("http://localhost:" + wireMock.port())
            .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
            .build(CubeWsClient.class);
  }

  @AfterEach
  void tearDown() {
    if (wireMock != null) {
      wireMock.stop();
    }
  }

  @Test
  void schema() {
    wireMock.stubFor(
        get(urlPathEqualTo("/occurrence/count/schema"))
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withBody(SCHEMA_RESPONSE)));

    List<Rollup> schema = client.getSchema();
    assertFalse(schema.isEmpty());
  }

  @Test
  void count() {
    wireMock.stubFor(
        get(urlPathEqualTo("/occurrence/count"))
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withBody("42")));

    Long count = client.count(Collections.emptyMap());
    assertEquals(42L, count);
  }
}
