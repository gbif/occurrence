package org.gbif.occurrence.search.heatmap;

import org.gbif.occurrence.search.heatmap.es.EsOccurrenceHeatmapResponse;

import javax.annotation.Nullable;

/**
 * Generic interface for Heatmap services.
 * @param <S> search engine search request type
 * @param <R> search engine search response type
 */
public interface OccurrenceHeatmapService<S,R> {

  /**
   * Provides a HeatMap aggregation based on GeoBounds.
   */
  EsOccurrenceHeatmapResponse.GeoBoundsResponse searchHeatMapGeoBounds(@Nullable OccurrenceHeatmapRequest request);

  /**
   * Provides a HeatMap aggregation based on a GeoCentroid.
   */
  EsOccurrenceHeatmapResponse.GeoCentroidResponse searchHeatMapGeoCentroid(@Nullable OccurrenceHeatmapRequest request);


  /**
   * Performs a search using the request and response types supported by the SearchEngine.
   * This method is used to perform 'native' queries on the SearchEngine.
   * @param searchRequest query
   * @param <S> search engine search request type
   * @param <R> search engine search response type
   * @return and search engine response
   */
   R searchOnEngine(S searchRequest);
}
