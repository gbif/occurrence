package org.gbif.occurrence.search.heatmap;

import javax.annotation.Nullable;

/**
 * Generic interface for heatmap services.
 * @param <T> response type
 */
public interface OccurrenceHeatmapService<T> {

  /**
   * Generic method that receives a general heatmap request and returns a response specific to the geo-spatial engine.
   * @param request generic heatmap reques

   * @return
   */
  T searchHeatMap(@Nullable OccurrenceHeatmapRequest request);
}
