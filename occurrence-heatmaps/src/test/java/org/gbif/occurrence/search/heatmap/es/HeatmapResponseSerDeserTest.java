package org.gbif.occurrence.search.heatmap.es;

import java.io.IOException;
import java.io.InputStream;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for serialization and deserialization of heatmap responses.
 */
public class HeatmapResponseSerDeserTest {

  private static final String TEST_JSON_FILE = "/json/es-heatmap.json";

  private static final ObjectMapper MAPPER = new ObjectMapper();


  /**
   * Tests that the the class can be deserialized from a test file.
   * @throws IOException opening the test file
   */
  @Test
  public void deserializationTest() throws IOException {
    try (InputStream  testFile = HeatmapResponseSerDeserTest.class.getResourceAsStream(TEST_JSON_FILE)) {
      JsonNode json = MAPPER.readTree(testFile);
      HeatmapResponse heatmapResponse = MAPPER.treeToValue(json.path("aggregations").path("heatmap"), HeatmapResponse.class);
      Assert.assertEquals(heatmapResponse.getBuckets().size(), 10);
    }
  }

}
