package org.gbif.occurrence.search.heatmap;

import org.gbif.occurrence.search.heatmap.es.EsOccurrenceHeatmapResponse;

import javax.annotation.Nullable;

/**
 * Generic interface for Heatmap services.
 */
public interface OccurrenceHeatmapService {

  /**
   * Provides a HeatMap aggregation based on GeoBounds.
   */
  EsOccurrenceHeatmapResponse.GeoBoundsResponse searchHeatMapGeoBounds(@Nullable OccurrenceHeatmapRequest request);

  /**
   * Provides a HeatMap aggregation based on a GeoCentroid.
   */
  EsOccurrenceHeatmapResponse.GeoCentroidResponse searchHeatMapGeoCentroid(@Nullable OccurrenceHeatmapRequest request);
}
