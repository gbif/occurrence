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
package org.gbif.occurrence.search.configuration;

import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.rest.client.species.NameUsageMatchResponse;
import org.gbif.rest.client.species.NameUsageMatchingService;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The name matching client works with the Feign annotations of the kvs client. */
class NameUsageMatchingServiceClientTest {

  private static final String RESPONSE =
      "{\"usage\":{\"key\":\"5219404\",\"name\":\"Puma concolor\",\"rank\":\"SPECIES\"},"
          + "\"diagnostics\":{\"matchType\":\"EXACT\",\"unknownField\":true}}";

  private HttpServer server;
  private final AtomicReference<String> requestUri = new AtomicReference<>();

  @BeforeEach
  void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          requestUri.set(exchange.getRequestURI().toString());
          byte[] body = RESPONSE.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        });
    server.start();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  @Test
  void matchesNames() {
    NameUsageMatchingService service =
        new OccurrenceSearchConfiguration()
            .nameUsageMatchingService("http://localhost:" + server.getAddress().getPort());

    NameUsageMatchResponse response =
        service.match(
            NameUsageMatchRequest.builder()
                .withChecklistKey("7ddf754f-d193-4cc9-b351-99906754a03b")
                .withScientificName("Puma concolor")
                .build());

    assertTrue(requestUri.get().startsWith("/v2/species/match?"), requestUri.get());
    assertTrue(requestUri.get().contains("scientificName=Puma%20concolor"), requestUri.get());
    assertTrue(requestUri.get().contains("checklistKey=7ddf754f-d193-4cc9-b351-99906754a03b"), requestUri.get());
    assertEquals("5219404", response.getUsage().getKey());
    assertEquals(NameUsageMatchResponse.MatchType.EXACT, response.getDiagnostics().getMatchType());
  }
}
