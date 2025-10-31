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
package org.gbif.search.heatmap.es;

import static org.gbif.occurrence.search.es.EsQueryUtils.HEADERS;
import static org.gbif.search.heatmap.es.EsHeatmapRequestBuilder.*;

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
import org.gbif.api.model.common.search.FacetedSearchRequest;
import org.gbif.api.model.common.search.PredicateSearchRequest;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.occurrence.search.SearchException;
import org.gbif.search.heatmap.HeatmapRequest;
import org.gbif.search.heatmap.HeatmapService;
import org.gbif.search.heatmap.occurrence.OccurrenceHeatmapRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/** Elasticsearch heatmap service. */
public abstract class BaseHeatmapsEsService<
        P extends SearchParameter,
        HR extends FacetedSearchRequest<P> & HeatmapRequest & PredicateSearchRequest>
    implements HeatmapService<SearchRequest, SearchResponse, HR> {

  private static final Logger LOG = LoggerFactory.getLogger(BaseHeatmapsEsService.class);

  private final RestHighLevelClient esClient;
  private final String esIndex;
  private final EsHeatmapRequestBuilder<P, HR> esHeatmapRequestBuilder;

  @Autowired
  public BaseHeatmapsEsService(
      RestHighLevelClient esClient,
      String esIndex,
      EsHeatmapRequestBuilder<P, HR> esHeatmapRequestBuilder) {
    this.esIndex = esIndex;
    this.esClient = esClient;
    this.esHeatmapRequestBuilder = esHeatmapRequestBuilder;
  }

  @Override
  public EsHeatmapResponse.GeoBoundsResponse searchHeatMapGeoBounds(@Nullable HR request) {
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
  public EsHeatmapResponse.GeoCentroidResponse searchHeatMapGeoCentroid(@Nullable HR request) {
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

  /** Transforms the {@link SearchResponse} into a {@link EsHeatmapResponse.GeoBoundsResponse}. */
  private static EsHeatmapResponse.GeoBoundsResponse parseGeoBoundsResponse(
      SearchResponse response) {
    ParsedGeoHashGrid heatmapAggs = response.getAggregations().get(HEATMAP_AGGS);

    List<EsHeatmapResponse.GeoBoundsGridBucket> buckets =
        heatmapAggs.getBuckets().stream()
            .map(
                b -> {
                  // build bucket
                  EsHeatmapResponse.GeoBoundsGridBucket bucket =
                      new EsHeatmapResponse.GeoBoundsGridBucket();
                  bucket.setKey(b.getKeyAsString());
                  bucket.setDocCount(b.getDocCount());

                  // build bounds
                  EsHeatmapResponse.Bounds bounds = new EsHeatmapResponse.Bounds();
                  ParsedGeoBounds cellAggs = b.getAggregations().get(CELL_AGGS);

                  // topLeft
                  EsHeatmapResponse.Coordinate topLeft = new EsHeatmapResponse.Coordinate();
                  topLeft.setLat(cellAggs.topLeft().getLat());
                  topLeft.setLon(cellAggs.topLeft().getLon());
                  bounds.setTopLeft(topLeft);

                  // bottomRight
                  EsHeatmapResponse.Coordinate bottomRight = new EsHeatmapResponse.Coordinate();
                  bottomRight.setLat(cellAggs.bottomRight().getLat());
                  bottomRight.setLon(cellAggs.bottomRight().getLon());
                  bounds.setBottomRight(bottomRight);

                  // build cell
                  EsHeatmapResponse.Cell cell = new EsHeatmapResponse.Cell();
                  cell.setBounds(bounds);
                  bucket.setCell(cell);

                  return bucket;
                })
            .collect(Collectors.toList());

    // build result
    EsHeatmapResponse.GeoBoundsResponse result = new EsHeatmapResponse.GeoBoundsResponse();
    result.setBuckets(buckets);

    return result;
  }

  /** Transforms a {@link SearchResponse} into a {@link EsHeatmapResponse.GeoCentroidResponse}. */
  private static EsHeatmapResponse.GeoCentroidResponse parseGeoCentroidResponse(
      SearchResponse response) {
    ParsedGeoHashGrid heatmapAggs = response.getAggregations().get(HEATMAP_AGGS);

    List<EsHeatmapResponse.GeoCentroidGridBucket> buckets =
        heatmapAggs.getBuckets().stream()
            .map(
                b -> {
                  // build bucket
                  EsHeatmapResponse.GeoCentroidGridBucket bucket =
                      new EsHeatmapResponse.GeoCentroidGridBucket();
                  bucket.setKey(b.getKeyAsString());
                  bucket.setDocCount(b.getDocCount());

                  ParsedGeoCentroid centroidAggs = b.getAggregations().get(CELL_AGGS);

                  // topLeft
                  EsHeatmapResponse.Coordinate centroid = new EsHeatmapResponse.Coordinate();
                  centroid.setLat(centroidAggs.centroid().getLat());
                  centroid.setLon(centroidAggs.centroid().getLon());

                  bucket.setCentroid(centroid);

                  return bucket;
                })
            .collect(Collectors.toList());

    // build result
    EsHeatmapResponse.GeoCentroidResponse result = new EsHeatmapResponse.GeoCentroidResponse();
    result.setBuckets(buckets);

    return result;
  }
}
