package org.gbif.occurrence.search.heatmap.es;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import org.apache.http.HttpEntity;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.gbif.common.shaded.com.google.common.collect.Iterables;
import org.gbif.occurrence.search.es.EsRequestBuilderBase;
import org.gbif.occurrence.search.es.OccurrenceEsField;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.gbif.occurrence.search.es.EsQueryUtils.*;

class EsHeatmapRequestBuilder extends EsRequestBuilderBase {

  static final String BOX_AGGS = "box";
  static final String HEATMAP_AGGS = "heatmap";
  static final String CELL_AGGS = "cell";

  private static final Logger LOG = LoggerFactory.getLogger(EsHeatmapRequestBuilder.class);

  private EsHeatmapRequestBuilder() {}

  static HttpEntity buildRequestBody(OccurrenceHeatmapRequest searchRequest) {
    return createEntity(buildQuery(searchRequest));
  }

  @VisibleForTesting
  static ObjectNode buildQuery(OccurrenceHeatmapRequest request) {
    // build request body
    ObjectNode requestBody = createObjectNode();
    requestBody.put(SIZE, 0);
    requestBody.put(QUERY, CREATE_NODE.apply(BOOL, createFilteredQuery(request)));
    requestBody.put(AGGS, createAggs(request));

    LOG.debug("ES query: {}", requestBody);

    return requestBody;
  }

  private static ObjectNode createFilteredQuery(OccurrenceHeatmapRequest request) {
    // create bool node
    ObjectNode bool = createObjectNode();

    // create filters
    ArrayNode filterNode = createArrayNode();

    // TODO: add geometry parameter from params map

    // adding term queries to bool
    buildTermQueries(request.getParameters())
        .ifPresent(termQueries -> termQueries.forEach(filterNode::add));

    bool.put(FILTER, filterNode);

    return bool;
  }

  private static ObjectNode createAggs(OccurrenceHeatmapRequest request) {

    // adding bounding box filter
    String[] coords =
        Iterables.toArray(Splitter.on(",").split(request.getGeometry()), String.class);

    ObjectNode bbox = createObjectNode();
    bbox.putPOJO("top_left", Arrays.asList(Double.valueOf(coords[0]), Double.valueOf(coords[3])));
    bbox.putPOJO(
        "bottom_right", Arrays.asList(Double.valueOf(coords[2]), Double.valueOf(coords[1])));

    ObjectNode boxAggs =
        CREATE_NODE.apply(
            FILTER,
            CREATE_NODE.apply(
                GEO_BOUNDING_BOX,
                CREATE_NODE.apply(OccurrenceEsField.COORDINATE_POINT.getFieldName(), bbox)));

    // create geohash_grid aggrs
    ObjectNode geohashGrid =
        CREATE_NODE.apply(FIELD, OccurrenceEsField.COORDINATE_POINT.getFieldName());
    geohashGrid.put(PRECISION, request.getZoom());

    ObjectNode heatmapAggs = createObjectNode();
    heatmapAggs.put(GEOHASH_GRID, geohashGrid);
    heatmapAggs.put(
        AGGS,
        CREATE_NODE.apply(
            CELL_AGGS,
            CREATE_NODE.apply(
                GEO_BOUNDS,
                CREATE_NODE.apply(FIELD, OccurrenceEsField.COORDINATE_POINT.getFieldName()))));

    boxAggs.put(AGGS, CREATE_NODE.apply(HEATMAP_AGGS, heatmapAggs));

    // add heatmap aggs
    return CREATE_NODE.apply(BOX_AGGS, boxAggs);
  }
}
