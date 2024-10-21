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

import static org.gbif.occurrence.search.es.EsQueryUtils.HEADERS;
import static org.gbif.occurrence.search.heatmap.es.EsHeatmapRequestBuilder.*;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.bucket.geogrid.ParsedGeoHashGrid;
import org.elasticsearch.search.aggregations.metrics.ParsedGeoBounds;
import org.elasticsearch.search.aggregations.metrics.ParsedGeoCentroid;
import org.gbif.occurrence.search.SearchException;
import org.gbif.occurrence.search.es.OccurrenceBaseEsFieldMapper;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequest;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapService;
import org.gbif.vocabulary.client.ConceptClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** Elasticsearch heatmap service. */
@Component
public class OccurrenceHeatmapsEsService
    implements OccurrenceHeatmapService<SearchRequest, SearchResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceHeatmapsEsService.class);

  private final RestHighLevelClient esClient;
  private final String esIndex;
  private final EsHeatmapRequestBuilder esHeatmapRequestBuilder;

  @Autowired
  public OccurrenceHeatmapsEsService(
      RestHighLevelClient esClient,
      String esIndex,
      OccurrenceBaseEsFieldMapper occurrenceBaseEsFieldMapper,
      ConceptClient conceptClient) {
    this.esIndex = esIndex;
    this.esClient = esClient;
    this.esHeatmapRequestBuilder = new EsHeatmapRequestBuilder(occurrenceBaseEsFieldMapper, conceptClient);
  }

  @Override
  public EsOccurrenceHeatmapResponse.GeoBoundsResponse searchHeatMapGeoBounds(
      @Nullable OccurrenceHeatmapRequest request) {
    Objects.requireNonNull(request);

    // build request, ensure mode is set.
    request.setMode(OccurrenceHeatmapRequest.Mode.GEO_BOUNDS);
    SearchRequest searchRequest = esHeatmapRequestBuilder.buildRequest(request, esIndex);
    LOG.debug("ES query: {}", searchRequest);

    try {
      return parseGeoBoundsResponse(esClient.search(searchRequest, HEADERS.get()));
    } catch (IOException e) {
      LOG.error("Error executing the search operation", e);
      throw new SearchException(e);
    }
  }

  @Override
  public EsOccurrenceHeatmapResponse.GeoCentroidResponse searchHeatMapGeoCentroid(
      @Nullable OccurrenceHeatmapRequest request) {
    Objects.requireNonNull(request);

    // build request, ensure mode is set.
    request.setMode(OccurrenceHeatmapRequest.Mode.GEO_CENTROID);
    SearchRequest searchRequest = esHeatmapRequestBuilder.buildRequest(request, esIndex);
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
   * Transforms the {@link SearchResponse} into a {@link
   * org.gbif.occurrence.search.heatmap.es.EsOccurrenceHeatmapResponse.GeoBoundsResponse}.
   */
  private static EsOccurrenceHeatmapResponse.GeoBoundsResponse parseGeoBoundsResponse(
      SearchResponse response) {
    ParsedGeoHashGrid heatmapAggs = response.getAggregations().get(HEATMAP_AGGS);

    List<EsOccurrenceHeatmapResponse.GeoBoundsGridBucket> buckets =
        heatmapAggs.getBuckets().stream()
            .map(
                b -> {
                  // build bucket
                  EsOccurrenceHeatmapResponse.GeoBoundsGridBucket bucket =
                      new EsOccurrenceHeatmapResponse.GeoBoundsGridBucket();
                  bucket.setKey(b.getKeyAsString());
                  bucket.setDocCount(b.getDocCount());

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
    EsOccurrenceHeatmapResponse.GeoBoundsResponse result =
        new EsOccurrenceHeatmapResponse.GeoBoundsResponse();
    result.setBuckets(buckets);

    return result;
  }

  /**
   * Transforms a {@link SearchResponse} into a {@link
   * org.gbif.occurrence.search.heatmap.es.EsOccurrenceHeatmapResponse.GeoCentroidResponse}.
   */
  private static EsOccurrenceHeatmapResponse.GeoCentroidResponse parseGeoCentroidResponse(
      SearchResponse response) {
    ParsedGeoHashGrid heatmapAggs = response.getAggregations().get(HEATMAP_AGGS);

    List<EsOccurrenceHeatmapResponse.GeoCentroidGridBucket> buckets =
        heatmapAggs.getBuckets().stream()
            .map(
                b -> {
                  // build bucket
                  EsOccurrenceHeatmapResponse.GeoCentroidGridBucket bucket =
                      new EsOccurrenceHeatmapResponse.GeoCentroidGridBucket();
                  bucket.setKey(b.getKeyAsString());
                  bucket.setDocCount(b.getDocCount());

                  ParsedGeoCentroid centroidAggs = b.getAggregations().get(CELL_AGGS);

                  // topLeft
                  EsOccurrenceHeatmapResponse.Coordinate centroid =
                      new EsOccurrenceHeatmapResponse.Coordinate();
                  centroid.setLat(centroidAggs.centroid().getLat());
                  centroid.setLon(centroidAggs.centroid().getLon());

                  bucket.setCentroid(centroid);

                  return bucket;
                })
            .collect(Collectors.toList());

    // build result
    EsOccurrenceHeatmapResponse.GeoCentroidResponse result =
        new EsOccurrenceHeatmapResponse.GeoCentroidResponse();
    result.setBuckets(buckets);

    return result;
  }
}
