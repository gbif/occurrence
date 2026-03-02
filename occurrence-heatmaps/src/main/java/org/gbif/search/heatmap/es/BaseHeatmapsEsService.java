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

import static org.gbif.search.heatmap.es.BaseEsHeatmapRequestBuilder.*;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.GeoBounds;
import co.elastic.clients.elasticsearch._types.GeoLocation;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import jakarta.annotation.Nullable;
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
    implements HeatmapService<SearchRequest, SearchResponse<Map<String, Object>>, HR> {

  private static final Logger LOG = LoggerFactory.getLogger(BaseHeatmapsEsService.class);

  private final ElasticsearchClient esClient;
  private final String esIndex;
  private final BaseEsHeatmapRequestBuilder<P, HR> esHeatmapRequestBuilder;

  @Autowired
  public BaseHeatmapsEsService(
      ElasticsearchClient esClient,
      String esIndex,
      BaseEsHeatmapRequestBuilder<P, HR> esHeatmapRequestBuilder) {
    this.esIndex = esIndex;
    this.esClient = esClient;
    this.esHeatmapRequestBuilder = esHeatmapRequestBuilder;
  }

  @Override
  public EsHeatmapResponse.GeoBoundsResponse searchHeatMapGeoBounds(@Nullable HR request) {
    Objects.requireNonNull(request);

    // build request, ensure mode is set.
    request.setMode(OccurrenceHeatmapRequest.Mode.GEO_BOUNDS);
    SearchRequest searchRequest = esHeatmapRequestBuilder.buildHeatmapRequest(request, esIndex);
    LOG.debug("ES query: {}", searchRequest);

    try {
      return parseGeoBoundsResponse(
          esClient.search(searchRequest, (Class<Map<String, Object>>) (Class<?>) Map.class));
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
    SearchRequest searchRequest = esHeatmapRequestBuilder.buildHeatmapRequest(request, esIndex);
    LOG.debug("ES query: {}", searchRequest);

    try {
      return parseGeoCentroidResponse(
          esClient.search(searchRequest, (Class<Map<String, Object>>) (Class<?>) Map.class));
    } catch (IOException e) {
      LOG.error("Error executing the search operation", e);
      throw new SearchException(e);
    }
  }

  @Override
  public SearchResponse<Map<String, Object>> searchOnEngine(SearchRequest searchRequest) {
    try {
      return esClient.search(searchRequest, (Class<Map<String, Object>>) (Class<?>) Map.class);
    } catch (IOException e) {
      LOG.error("Error executing the search operation", e);
      throw new SearchException(e);
    }
  }

  /** Transforms the {@link SearchResponse} into a {@link EsHeatmapResponse.GeoBoundsResponse}. */
  private static EsHeatmapResponse.GeoBoundsResponse parseGeoBoundsResponse(
      SearchResponse<Map<String, Object>> response) {
    Aggregate heatmapAggs = response.aggregations().get(HEATMAP_AGGS);
    if (heatmapAggs == null || !heatmapAggs.isGeohashGrid()) {
      EsHeatmapResponse.GeoBoundsResponse empty = new EsHeatmapResponse.GeoBoundsResponse();
      empty.setBuckets(List.of());
      return empty;
    }

    List<EsHeatmapResponse.GeoBoundsGridBucket> buckets =
        heatmapAggs.geohashGrid().buckets().array().stream()
            .map(
                b -> {
                  // build bucket
                  EsHeatmapResponse.GeoBoundsGridBucket bucket =
                      new EsHeatmapResponse.GeoBoundsGridBucket();
                  bucket.setKey(b.key());
                  bucket.setDocCount(b.docCount());

                  // build bounds
                  EsHeatmapResponse.Bounds bounds = new EsHeatmapResponse.Bounds();
                  Aggregate cellAggs = b.aggregations().get(CELL_AGGS);
                  if (cellAggs != null && cellAggs.isGeoBounds()) {
                    GeoBounds geoBounds = cellAggs.geoBounds().bounds();
                    populateBounds(bounds, geoBounds);
                  }

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
      SearchResponse<Map<String, Object>> response) {
    Aggregate heatmapAggs = response.aggregations().get(HEATMAP_AGGS);
    if (heatmapAggs == null || !heatmapAggs.isGeohashGrid()) {
      EsHeatmapResponse.GeoCentroidResponse empty = new EsHeatmapResponse.GeoCentroidResponse();
      empty.setBuckets(List.of());
      return empty;
    }

    List<EsHeatmapResponse.GeoCentroidGridBucket> buckets =
        heatmapAggs.geohashGrid().buckets().array().stream()
            .map(
                b -> {
                  // build bucket
                  EsHeatmapResponse.GeoCentroidGridBucket bucket =
                      new EsHeatmapResponse.GeoCentroidGridBucket();
                  bucket.setKey(b.key());
                  bucket.setDocCount(b.docCount());

                  Aggregate centroidAggs = b.aggregations().get(CELL_AGGS);

                  EsHeatmapResponse.Coordinate centroid = new EsHeatmapResponse.Coordinate();
                  if (centroidAggs != null && centroidAggs.isGeoCentroid()) {
                    GeoLocation location = centroidAggs.geoCentroid().location();
                    centroid.setLat(readLat(location));
                    centroid.setLon(readLon(location));
                  }

                  bucket.setCentroid(centroid);

                  return bucket;
                })
            .collect(Collectors.toList());

    // build result
    EsHeatmapResponse.GeoCentroidResponse result = new EsHeatmapResponse.GeoCentroidResponse();
    result.setBuckets(buckets);

    return result;
  }

  private static void populateBounds(EsHeatmapResponse.Bounds bounds, GeoBounds geoBounds) {
    EsHeatmapResponse.Coordinate topLeft = new EsHeatmapResponse.Coordinate();
    EsHeatmapResponse.Coordinate bottomRight = new EsHeatmapResponse.Coordinate();

    if (geoBounds.isCoords()) {
      topLeft.setLat(geoBounds.coords().top());
      topLeft.setLon(geoBounds.coords().left());
      bottomRight.setLat(geoBounds.coords().bottom());
      bottomRight.setLon(geoBounds.coords().right());
    } else if (geoBounds.isTlbr()) {
      topLeft.setLat(readLat(geoBounds.tlbr().topLeft()));
      topLeft.setLon(readLon(geoBounds.tlbr().topLeft()));
      bottomRight.setLat(readLat(geoBounds.tlbr().bottomRight()));
      bottomRight.setLon(readLon(geoBounds.tlbr().bottomRight()));
    }

    bounds.setTopLeft(topLeft);
    bounds.setBottomRight(bottomRight);
  }

  private static double readLat(GeoLocation geoLocation) {
    if (geoLocation == null) {
      return 0D;
    }
    if (geoLocation.isLatlon()) {
      return geoLocation.latlon().lat();
    }
    if (geoLocation.isCoords() && geoLocation.coords().size() > 1) {
      return geoLocation.coords().get(1);
    }
    return 0D;
  }

  private static double readLon(GeoLocation geoLocation) {
    if (geoLocation == null) {
      return 0D;
    }
    if (geoLocation.isLatlon()) {
      return geoLocation.latlon().lon();
    }
    if (geoLocation.isCoords() && !geoLocation.coords().isEmpty()) {
      return geoLocation.coords().get(0);
    }
    return 0D;
  }

}
