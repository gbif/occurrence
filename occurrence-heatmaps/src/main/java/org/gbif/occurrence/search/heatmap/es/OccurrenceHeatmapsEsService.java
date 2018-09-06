package org.gbif.occurrence.search.heatmap.es;

import com.google.inject.Inject;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.geogrid.ParsedGeoHashGrid;
import org.elasticsearch.search.aggregations.metrics.geobounds.ParsedGeoBounds;
import org.gbif.occurrence.search.SearchException;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequest;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.gbif.occurrence.search.es.EsQueryUtils.HEADERS;
import static org.gbif.occurrence.search.heatmap.es.EsHeatmapRequestBuilder.*;

public class OccurrenceHeatmapsEsService
    implements OccurrenceHeatmapService<EsOccurrenceHeatmapResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceHeatmapsEsService.class);

  private final RestHighLevelClient esClient;
  private final String esIndex;

  @Inject
  public OccurrenceHeatmapsEsService(RestHighLevelClient esClient, String esIndex) {
    this.esIndex = esIndex;
    this.esClient = esClient;
  }

  @Override
  public EsOccurrenceHeatmapResponse searchHeatMap(@Nullable OccurrenceHeatmapRequest request) {
    Objects.requireNonNull(request);

    // build request
    SearchRequest searchRequest = EsHeatmapRequestBuilder.buildRequest(request, esIndex);
    LOG.debug("ES query: {}", searchRequest);

    // perform search
    SearchResponse response = null;
    try {
      response = esClient.search(searchRequest, HEADERS.get());
    } catch (IOException e) {
      LOG.error("Error executing the search operation", e);
      throw new SearchException(e);
    }

    // parse response
    return parseResponse(response);
  }

  private static EsOccurrenceHeatmapResponse parseResponse(SearchResponse response) {
    ParsedFilter boxAggs = response.getAggregations().get(BOX_AGGS);
    ParsedGeoHashGrid heatmapAggs = boxAggs.getAggregations().get(HEATMAP_AGGS);

    List<EsOccurrenceHeatmapResponse.GeoGridBucket> buckets =
        heatmapAggs
            .getBuckets()
            .stream()
            .map(
                b -> {
                  // build bucket
                  EsOccurrenceHeatmapResponse.GeoGridBucket bucket =
                      new EsOccurrenceHeatmapResponse.GeoGridBucket();
                  bucket.setKey(b.getKeyAsString());
                  bucket.setDocCount((int) b.getDocCount());

                  // build bounds
                  EsOccurrenceHeatmapResponse.Bounds bounds =
                      new EsOccurrenceHeatmapResponse.Bounds();
                  ParsedGeoBounds cellAggs = b.getAggregations().get(CELL_AGGS);
                  // topLeft
                  EsOccurrenceHeatmapResponse.Coordinate topLeft =
                      new EsOccurrenceHeatmapResponse.Coordinate();
                  topLeft.setLat(cellAggs.topLeft().getLat());
                  topLeft.setLon(cellAggs.topLeft().getLon());
                  bounds.setTopLeft(topLeft);
                  // bottomRight
                  EsOccurrenceHeatmapResponse.Coordinate bottomRight =
                      new EsOccurrenceHeatmapResponse.Coordinate();
                  bottomRight.setLat(cellAggs.bottomRight().getLat());
                  bottomRight.setLon(cellAggs.bottomRight().getLon());
                  bounds.setBottomRight(bottomRight);

                  // build cell
                  EsOccurrenceHeatmapResponse.Cell cell = new EsOccurrenceHeatmapResponse.Cell();
                  cell.setBounds(bounds);
                  bucket.setCell(cell);

                  return bucket;
                })
            .collect(Collectors.toList());

    // build result
    EsOccurrenceHeatmapResponse result = new EsOccurrenceHeatmapResponse();
    result.setBuckets(buckets);

    return result;
  }
}
