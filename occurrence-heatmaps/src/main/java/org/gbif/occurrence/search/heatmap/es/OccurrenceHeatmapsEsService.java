package org.gbif.occurrence.search.heatmap.es;

import com.google.inject.Inject;
import org.apache.http.HttpEntity;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.gbif.common.search.SearchException;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequest;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.gbif.occurrence.search.es.EsQueryUtils.HEADERS;
import static org.gbif.occurrence.search.es.EsQueryUtils.SEARCH_ENDPOINT;

public class OccurrenceHeatmapsEsService
    implements OccurrenceHeatmapService<EsOccurrenceHeatmapResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceHeatmapsEsService.class);
  private static final ObjectReader JSON_READER = new ObjectMapper().reader(Map.class);

  private final RestClient esClient;
  private final String esIndex;

  @Inject
  public OccurrenceHeatmapsEsService(RestClient esClient, String esIndex) {
    this.esIndex = esIndex;
    this.esClient = esClient;
  }

  @Override
  public EsOccurrenceHeatmapResponse searchHeatMap(@Nullable OccurrenceHeatmapRequest request) {

    HttpEntity requestBody = EsHeatmapRequestBuilder.buildRequestBody(request);
    Response response = null;
    try {
      response =
        esClient.performRequest(
          "GET",
          SEARCH_ENDPOINT.apply(esIndex),
          Collections.emptyMap(),
          requestBody,
          HEADERS.get());
    } catch (IOException e) {
      LOG.error("Error executing the search operation", e);
      throw new SearchException(e);
    }

    JsonNode jsonResponse = null;
    try {
      jsonResponse = JSON_READER.readTree(response.getEntity().getContent());
      return JSON_READER.treeToValue(jsonResponse.path("aggregations").path("heatmap"), EsOccurrenceHeatmapResponse.class);
    } catch (IOException e) {
      LOG.error("Error reading ES response", e);
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }
}
