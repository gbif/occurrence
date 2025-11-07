package org.gbif.search.heatmap;

import jakarta.servlet.http.HttpServletRequest;

import org.gbif.occurrence.search.cache.PredicateCacheService;

public interface HeatmapRequestProvider<R extends HeatmapRequest> {

  R buildOccurrenceHeatmapRequest(HttpServletRequest request);

  PredicateCacheService getPredicateCacheService();
}
