package org.gbif.occurrence.search.heatmap.es;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.geobounds.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.geocentroid.GeoCentroidAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.gbif.common.shaded.com.google.common.collect.Iterables;
import org.gbif.occurrence.search.es.EsSearchRequestBuilder;
import org.gbif.occurrence.search.es.OccurrenceEsField;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequest;

class EsHeatmapRequestBuilder {

  static final String BOX_AGGS = "box";
  static final String HEATMAP_AGGS = "heatmap";
  static final String CELL_AGGS = "cell";

  //Mapping of predefined zoom levels
  private static final int[] PRECISION_LOOKUP = new int[]{3, 3, 3, 3, 4, 4, 4, 5, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10};

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

    // add hasCoordinate to the filter and create query
    request.addHasCoordinateFilter(true);
    EsSearchRequestBuilder.buildQueryNode(request).ifPresent(searchSourceBuilder::query);

    // add aggs
    searchSourceBuilder.aggregation(buildAggs(request));

    return esRequest;
  }

  private static AggregationBuilder buildAggs(OccurrenceHeatmapRequest request) {
    // adding bounding box filter
    String[] coords =
        Iterables.toArray(Splitter.on(",").split(request.getGeometry()), String.class);

    GeoBoundingBoxQueryBuilder geoBoundingBoxQuery =
        QueryBuilders.geoBoundingBoxQuery(OccurrenceEsField.COORDINATE_POINT.getFieldName())
            .setCorners(
                Double.parseDouble(coords[3]),
                Double.parseDouble(coords[0]),
                Double.parseDouble(coords[1]),
                Double.parseDouble(coords[2]));

    FilterAggregationBuilder filterAggs = AggregationBuilders.filter(BOX_AGGS, geoBoundingBoxQuery);

    GeoGridAggregationBuilder geoGridAggs =
        AggregationBuilders.geohashGrid(HEATMAP_AGGS)
            .field(OccurrenceEsField.COORDINATE_POINT.getFieldName())
            .precision(PRECISION_LOOKUP[Math.min(request.getZoom(), PRECISION_LOOKUP.length - 1)]);

    if (OccurrenceHeatmapRequest.Mode.GEO_CENTROID == request.getMode()) {
      GeoCentroidAggregationBuilder geoCentroidAggs = AggregationBuilders.geoCentroid(CELL_AGGS)
                                                        .field(OccurrenceEsField.COORDINATE_POINT.getFieldName());
      geoGridAggs.subAggregation(geoCentroidAggs);
    } else {
      GeoBoundsAggregationBuilder geoBoundsAggs = AggregationBuilders.geoBounds(CELL_AGGS)
                                                    .field(OccurrenceEsField.COORDINATE_POINT.getFieldName());
      geoGridAggs.subAggregation(geoBoundsAggs);
    }

    filterAggs.subAggregation(geoGridAggs);

    return filterAggs;
  }
}
