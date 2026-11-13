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

import org.gbif.api.model.common.search.FacetedSearchRequest;
import org.gbif.api.model.common.search.PredicateSearchRequest;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.occurrence.search.es.BaseEsSearchRequestBuilder;
import org.gbif.predicate.query.EsFieldMapper;
import org.gbif.predicate.query.EsQueryVisitor;
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.gbif.search.heatmap.HeatmapRequest;
import org.gbif.search.heatmap.occurrence.OccurrenceHeatmapRequest;
import org.gbif.vocabulary.client.ConceptClient;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;

public abstract class BaseEsHeatmapRequestBuilder<
        P extends SearchParameter,
        R extends FacetedSearchRequest<P> & HeatmapRequest & PredicateSearchRequest>
    extends BaseEsSearchRequestBuilder<P, R> {

  public static final String HEATMAP_AGGS = "heatmap";
  public static final String CELL_AGGS = "cell";

  // Every request used to get at least this many buckets (Math.max with bucketLimit), and larger
  // bucketLimit values let callers ask for unbounded aggregations, so this is now the only size.
  private static final int MAX_BUCKETS = 50000;

  // Mapping of predefined zoom levels
  private static final int[] PRECISION_LOOKUP =
      new int[] {2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10};

  public BaseEsHeatmapRequestBuilder(
      EsFieldMapper<P> esFieldMapper,
      ConceptClient conceptClient,
      NameUsageMatchingService nameUsageMatchingService,
      EsQueryVisitor<P> esQueryVisitor,
      String defaultChecklistKey,
      int defaultShardSize) {
    super(esFieldMapper, conceptClient, nameUsageMatchingService, esQueryVisitor, defaultChecklistKey, defaultShardSize);
  }

  @VisibleForTesting
  public SearchRequest buildHeatmapRequest(R request, String index) {
    // add the geometry filter
    double[] coords = parseBoundingBox(request.getGeometry());

    double top = coords[3];
    double left = coords[0];
    double bottom = coords[1];
    double right = coords[2];

    List<Query> filters = new ArrayList<>();
    filters.add(
        Query.of(
            q ->
                q.geoBoundingBox(
                    g ->
                        g.field(esFieldMapper.getGeoDistanceField())
                            .boundingBox(
                                b ->
                                    b.coords(
                                        c ->
                                            c.top(top)
                                                .left(left)
                                                .bottom(bottom)
                                                .right(right))))));

    getParam(OccurrenceSearchParameter.HAS_COORDINATE.name())
        .ifPresent(
            param -> {
              filters.add(
                  Query.of(
                      q ->
                          q.term(
                              t -> t.field(esFieldMapper.getSearchFieldName(param)).value(true))));
            });

    // add query
    if (request.getPredicate() != null) { // is a predicate search
      buildQuery(request).ifPresent(filters::add);
    } else {
      // add hasCoordinate to the filter and create query
      addBuildQueryNodeFilter(request, filters);
    }

    return SearchRequest.of(
        s ->
            s.index(index)
                .size(0)
                .query(q -> q.bool(b -> b.filter(filters)))
                .aggregations(buildAggs(request)));
  }

  /** Parses a "left,bottom,right,top" bounding box. */
  private static double[] parseBoundingBox(String geometry) {
    String[] parts = geometry != null ? geometry.split(",") : new String[0];
    if (parts.length != 4) {
      throw new IllegalArgumentException("geom must be a bounding box: left,bottom,right,top");
    }
    double[] coords = new double[4];
    try {
      for (int i = 0; i < 4; i++) {
        coords[i] = Double.parseDouble(parts[i].trim());
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("geom must be a bounding box: left,bottom,right,top", e);
    }
    return coords;
  }

  private Map<String, Aggregation> buildAggs(R request) {
    String geoDistanceField = esFieldMapper.getGeoDistanceField();
    Aggregation cellAggregation =
        OccurrenceHeatmapRequest.Mode.GEO_CENTROID == request.getMode()
            ? Aggregation.of(a -> a.geoCentroid(c -> c.field(geoDistanceField)))
            : Aggregation.of(a -> a.geoBounds(b -> b.field(geoDistanceField)));

    Aggregation heatmapAggregation =
        Aggregation.of(
            a ->
                a.geohashGrid(
                        g ->
                            g.field(geoDistanceField)
                                .precision(
                                    p ->
                                        p.geohashLength(
                                            PRECISION_LOOKUP[
                                                Math.max(0, Math.min(request.getZoom(), PRECISION_LOOKUP.length - 1))]))
                                .size(MAX_BUCKETS))
                    .aggregations(CELL_AGGS, cellAggregation));

    Map<String, Aggregation> aggregations = new LinkedHashMap<>();
    aggregations.put(HEATMAP_AGGS, heatmapAggregation);
    return aggregations;
  }

  /**
   * Adds the query from buildQueryNode to the filter list. buildQueryNode returns the new
   * Elasticsearch client's Query type, so we add it directly instead of re-serializing.
   */
  private void addBuildQueryNodeFilter(R request, List<Query> filters) {
    buildQueryNode(request).ifPresent(filters::add);
  }
}
