package org.gbif.occurrence.search.heatmap.es;

import com.google.inject.Inject;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.bucket.filter.ParsedFilter;
import org.elasticsearch.search.aggregations.bucket.geogrid.ParsedGeoHashGrid;
import org.elasticsearch.search.aggregations.metrics.geobounds.ParsedGeoBounds;
import org.elasticsearch.search.aggregations.metrics.geocentroid.ParsedGeoCentroid;
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

/**
 * Elasticsearch heatmap service.
 */
public class OccurrenceHeatmapsEsService implements OccurrenceHeatmapService<SearchRequest,SearchResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceHeatmapsEsService.class);

  private final RestHighLevelClient esClient;
  private final String esIndex;

  @Inject
  public OccurrenceHeatmapsEsService(RestHighLevelClient esClient, String esIndex) {
    this.esIndex = esIndex;
    this.esClient = esClient;
  }

  @Override
  public EsOccurrenceHeatmapResponse.GeoBoundsResponse searchHeatMapGeoBounds(@Nullable OccurrenceHeatmapRequest request) {
    Objects.requireNonNull(request);

    // build request
    SearchRequest searchRequest = EsHeatmapRequestBuilder.buildRequest(request, esIndex);
    LOG.debug("ES query: {}", searchRequest);

    try {
      return parseGeoBoundsResponse(esClient.search(searchRequest, HEADERS.get()));
    } catch (IOException e) {
      LOG.error("Error executing the search operation", e);
      throw new SearchException(e);
    }

  }

  @Override
  public EsOccurrenceHeatmapResponse.GeoCentroidResponse searchHeatMapGeoCentroid(@Nullable OccurrenceHeatmapRequest request) {
    Objects.requireNonNull(request);

    // build request
    SearchRequest searchRequest = EsHeatmapRequestBuilder.buildRequest(request, esIndex);
    LOG.debug("ES query: {}", searchRequest);

    try {
      return parseGeoCentroidResponse(esClient.search(searchRequest, HEADERS.get()));
    } catch (IOException e) {
      LOG.error("Error executing the search operation", e);
      throw new SearchException(e);
    }

  }

  @Override
  public SearchResponse searchOnEngine(SearchRequest searchRequest) {
    try {
      return esClient.search(searchRequest, HEADERS.get());
    } catch (IOException e) {
      LOG.error("Error executing the search operation", e);
      throw new SearchException(e);
    }
  }

  /**
   * Transforms the {@link SearchResponse} into a {@link org.gbif.occurrence.search.heatmap.es.EsOccurrenceHeatmapResponse.GeoBoundsResponse}.
   */
  private static EsOccurrenceHeatmapResponse.GeoBoundsResponse parseGeoBoundsResponse(SearchResponse response) {
    ParsedFilter boxAggs = response.getAggregations().get(BOX_AGGS);
    ParsedGeoHashGrid heatmapAggs = boxAggs.getAggregations().get(HEATMAP_AGGS);

    List<EsOccurrenceHeatmapResponse.GeoBoundsGridBucket> buckets =
        heatmapAggs
            .getBuckets()
            .stream()
            .map(
                b -> {
                  // build bucket
                  EsOccurrenceHeatmapResponse.GeoBoundsGridBucket bucket = new EsOccurrenceHeatmapResponse.GeoBoundsGridBucket();
                  bucket.setKey(b.getKeyAsString());
                  bucket.setDocCount(b.getDocCount());

                  // build bounds
                  EsOccurrenceHeatmapResponse.Bounds bounds = new EsOccurrenceHeatmapResponse.Bounds();
                  ParsedGeoBounds cellAggs = b.getAggregations().get(CELL_AGGS);

                  // topLeft
                  EsOccurrenceHeatmapResponse.Coordinate topLeft = new EsOccurrenceHeatmapResponse.Coordinate();
                  topLeft.setLat(cellAggs.topLeft().getLat());
                  topLeft.setLon(cellAggs.topLeft().getLon());
                  bounds.setTopLeft(topLeft);

                  // bottomRight
                  EsOccurrenceHeatmapResponse.Coordinate bottomRight = new EsOccurrenceHeatmapResponse.Coordinate();
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
    EsOccurrenceHeatmapResponse.GeoBoundsResponse result = new EsOccurrenceHeatmapResponse.GeoBoundsResponse();
    result.setBuckets(buckets);

    return result;
  }


  /**
   * Transforms a {@link SearchResponse} into a {@link org.gbif.occurrence.search.heatmap.es.EsOccurrenceHeatmapResponse.GeoCentroidResponse}.
   */
  private static EsOccurrenceHeatmapResponse.GeoCentroidResponse parseGeoCentroidResponse(SearchResponse response) {
    ParsedFilter boxAggs = response.getAggregations().get(BOX_AGGS);
    ParsedGeoHashGrid heatmapAggs = boxAggs.getAggregations().get(HEATMAP_AGGS);

    List<EsOccurrenceHeatmapResponse.GeoCentroidGridBucket> buckets =
      heatmapAggs
        .getBuckets()
        .stream()
        .map(
          b -> {
            // build bucket
            EsOccurrenceHeatmapResponse.GeoCentroidGridBucket bucket = new EsOccurrenceHeatmapResponse.GeoCentroidGridBucket();
            bucket.setKey(b.getKeyAsString());
            bucket.setDocCount(b.getDocCount());


            ParsedGeoCentroid centroidAggs = b.getAggregations().get(CELL_AGGS);

            // topLeft
            EsOccurrenceHeatmapResponse.Coordinate centroid = new EsOccurrenceHeatmapResponse.Coordinate();
            centroid.setLat(centroidAggs.centroid().getLat());
            centroid.setLon(centroidAggs.centroid().getLon());

            bucket.setCentroid(centroid);

            return bucket;
          })
        .collect(Collectors.toList());

    // build result
    EsOccurrenceHeatmapResponse.GeoCentroidResponse result = new EsOccurrenceHeatmapResponse.GeoCentroidResponse();
    result.setBuckets(buckets);

    return result;
  }
}
