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
package org.gbif.occurrence.search.heatmap.es;

import java.io.IOException;
import java.io.InputStream;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test class for serialization and deserialization of heatmap responses.
 */
public class EsOccurrenceHeatmapResponseSerDeserTest {

  private static final String TEST_JSON_FILE = "/json/es-heatmap.json";

  private static final ObjectMapper MAPPER = new ObjectMapper();


  /**
   * Tests that the the class can be deserialized from a test file.
   * @throws IOException opening the test file
   */
  @Test
  public void deserializationTest() throws IOException {
    try (InputStream  testFile = EsOccurrenceHeatmapResponseSerDeserTest.class.getResourceAsStream(TEST_JSON_FILE)) {
      JsonNode json = MAPPER.readTree(testFile);
      EsOccurrenceHeatmapResponse.GeoBoundsResponse esOccurrenceHeatmapResponse = MAPPER.treeToValue(json.path("aggregations").path("heatmap"), EsOccurrenceHeatmapResponse.GeoBoundsResponse.class);
      assertEquals(esOccurrenceHeatmapResponse.getBuckets().size(), 10);
    }
  }

}
