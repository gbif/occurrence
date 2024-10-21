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

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.occurrence.search.es.EsSearchRequestBuilder;
import org.gbif.occurrence.search.es.OccurrenceBaseEsFieldMapper;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequest;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.google.common.annotations.VisibleForTesting;

import org.gbif.vocabulary.client.ConceptClient;

class EsHeatmapRequestBuilder {

  static final String HEATMAP_AGGS = "heatmap";
  static final String CELL_AGGS = "cell";

  //Mapping of predefined zoom levels
  private static final int[] PRECISION_LOOKUP = new int[]{2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10};

  private final OccurrenceBaseEsFieldMapper occurrenceBaseEsFieldMapper;
  private final EsSearchRequestBuilder esSearchRequestBuilder;

  EsHeatmapRequestBuilder(OccurrenceBaseEsFieldMapper occurrenceBaseEsFieldMapper, ConceptClient conceptClient) {
    this.occurrenceBaseEsFieldMapper = occurrenceBaseEsFieldMapper;
    this.esSearchRequestBuilder = new EsSearchRequestBuilder(occurrenceBaseEsFieldMapper, conceptClient);
  }

  @VisibleForTesting
  SearchRequest buildRequest(OccurrenceHeatmapRequest request, String index) {
    // build request body
    SearchRequest esRequest = new SearchRequest();
    esRequest.indices(index);

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    esRequest.source(searchSourceBuilder);

    // size 0
    searchSourceBuilder.size(0);

    // add the geometry filter
    String[] coords = request.getGeometry().split(",");

    double top = Double.parseDouble(coords[3]);
    double left = Double.parseDouble(coords[0]);
    double bottom = Double.parseDouble(coords[1]);
    double right = Double.parseDouble(coords[2]);

    BoolQueryBuilder bool = QueryBuilders.boolQuery();
    bool.filter().add(QueryBuilders.geoBoundingBoxQuery(occurrenceBaseEsFieldMapper.getGeoDistanceField())
      .setCorners(top, left, bottom, right));
    bool.filter().add(QueryBuilders.termQuery(
      occurrenceBaseEsFieldMapper.getSearchFieldName(OccurrenceSearchParameter.HAS_COORDINATE), true));

    // add query
    if (request.getPredicate() != null) { //is a predicate search
      esSearchRequestBuilder.buildQuery(request).ifPresent(bool.filter()::add);
    } else {
      // add hasCoordinate to the filter and create query
      esSearchRequestBuilder.buildQueryNode(request).ifPresent(bool.filter()::add);
    }

    searchSourceBuilder.query(bool);

    // add aggs
    searchSourceBuilder.aggregation(buildAggs(request));

    return esRequest;
  }

  private AggregationBuilder buildAggs(OccurrenceHeatmapRequest request) {
    String geoDistanceField = occurrenceBaseEsFieldMapper.getGeoDistanceField();
    GeoGridAggregationBuilder geoGridAggs =
        AggregationBuilders.geohashGrid(HEATMAP_AGGS)
            .field(geoDistanceField)
            .precision(PRECISION_LOOKUP[Math.min(request.getZoom(), PRECISION_LOOKUP.length - 1)])
            .size(Math.max(request.getBucketLimit(), 50000));

    if (OccurrenceHeatmapRequest.Mode.GEO_CENTROID == request.getMode()) {
      GeoCentroidAggregationBuilder geoCentroidAggs = AggregationBuilders.geoCentroid(CELL_AGGS)
                                                        .field(geoDistanceField);
      geoGridAggs.subAggregation(geoCentroidAggs);
    } else {
      GeoBoundsAggregationBuilder geoBoundsAggs = AggregationBuilders.geoBounds(CELL_AGGS)
                                                    .field(geoDistanceField);
      geoGridAggs.subAggregation(geoBoundsAggs);
    }

    return geoGridAggs;
  }
}
