package org.gbif.occurrence.search.heatmap.es;

import com.google.inject.Inject;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.gbif.occurrence.search.SearchException;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequest;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

import static org.gbif.occurrence.search.es.EsQueryUtils.AGGREGATIONS;
import static org.gbif.occurrence.search.es.EsQueryUtils.HEADERS;
import static org.gbif.occurrence.search.heatmap.es.EsHeatmapRequestBuilder.BOX_AGGS;
import static org.gbif.occurrence.search.heatmap.es.EsHeatmapRequestBuilder.HEATMAP_AGGS;

public class OccurrenceHeatmapsEsService
    implements OccurrenceHeatmapService<EsOccurrenceHeatmapResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceHeatmapsEsService.class);
  private static final ObjectReader JSON_READER = new ObjectMapper().reader(Map.class);

  private final RestHighLevelClient esClient;
  private final String esIndex;

  @Inject
  public OccurrenceHeatmapsEsService(RestHighLevelClient esClient, String esIndex) {
    this.esIndex = esIndex;
    this.esClient = esClient;
  }

  @Override
  public EsOccurrenceHeatmapResponse searchHeatMap(@Nullable OccurrenceHeatmapRequest request) {

    SearchRequest searchRequest = EsHeatmapRequestBuilder.buildRequest(request, esIndex);
    SearchResponse response = null;
    try {
      response = esClient.search(searchRequest, HEADERS.get());
    } catch (IOException e) {
      LOG.error("Error executing the search operation", e);
      throw new SearchException(e);
    }

    try {
      JsonNode jsonResponse = JSON_READER.readTree(response.toString());
      return JSON_READER.treeToValue(
          jsonResponse.path(AGGREGATIONS).path(BOX_AGGS).path(HEATMAP_AGGS),
          EsOccurrenceHeatmapResponse.class);
    } catch (IOException e) {
      LOG.error("Error reading ES response", e);
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }
}
