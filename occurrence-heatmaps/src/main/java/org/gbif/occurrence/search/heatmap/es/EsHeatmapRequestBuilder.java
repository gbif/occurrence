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
import org.gbif.occurrence.search.es.EsQueryUtils;
import org.gbif.occurrence.search.es.EsSearchRequestBuilder;
import org.gbif.occurrence.search.es.OccurrenceEsField;
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

class EsHeatmapRequestBuilder {

  static final String BOX_AGGS = "box";
  static final String HEATMAP_AGGS = "heatmap";
  static final String CELL_AGGS = "cell";

  //Mapping of predefined zoom levels
  private static final int[] PRECISION_LOOKUP = new int[]{2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10};

  private EsHeatmapRequestBuilder() {}

  @VisibleForTesting
  static SearchRequest buildRequest(OccurrenceHeatmapRequest request, String index) {
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
    bool.filter().add(QueryBuilders.geoBoundingBoxQuery(OccurrenceEsField.COORDINATE_POINT.getSearchFieldName())
      .setCorners(top, left, bottom, right));
    bool.filter().add(QueryBuilders.termQuery(
      EsQueryUtils.SEARCH_TO_ES_MAPPING.get(OccurrenceSearchParameter.HAS_COORDINATE).getSearchFieldName(), true));

    // add query
    if (request.getPredicate() != null) { //is a predicate search
      EsSearchRequestBuilder.buildQuery(request).ifPresent(bool.filter()::add);
    } else {
      // add hasCoordinate to the filter and create query
      EsSearchRequestBuilder.buildQueryNode(request).ifPresent(bool.filter()::add);
    }


    searchSourceBuilder.query(bool);

    // add aggs
    searchSourceBuilder.aggregation(buildAggs(request));

    return esRequest;
  }

  private static AggregationBuilder buildAggs(OccurrenceHeatmapRequest request) {
    GeoGridAggregationBuilder geoGridAggs =
        AggregationBuilders.geohashGrid(HEATMAP_AGGS)
            .field(OccurrenceEsField.COORDINATE_POINT.getSearchFieldName())
            .precision(PRECISION_LOOKUP[Math.min(request.getZoom(), PRECISION_LOOKUP.length - 1)])
            .size(Math.max(request.getBucketLimit(), 50000));

    if (OccurrenceHeatmapRequest.Mode.GEO_CENTROID == request.getMode()) {
      GeoCentroidAggregationBuilder geoCentroidAggs = AggregationBuilders.geoCentroid(CELL_AGGS)
                                                        .field(OccurrenceEsField.COORDINATE_POINT.getSearchFieldName());
      geoGridAggs.subAggregation(geoCentroidAggs);
    } else {
      GeoBoundsAggregationBuilder geoBoundsAggs = AggregationBuilders.geoBounds(CELL_AGGS)
                                                    .field(OccurrenceEsField.COORDINATE_POINT.getSearchFieldName());
      geoGridAggs.subAggregation(geoBoundsAggs);
    }

    return geoGridAggs;
  }
}
