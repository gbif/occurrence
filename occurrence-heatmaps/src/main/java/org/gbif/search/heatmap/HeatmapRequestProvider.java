package org.gbif.search.heatmap;

import jakarta.servlet.http.HttpServletRequest;

public interface HeatmapRequestProvider<R extends HeatmapRequest> {

  R buildOccurrenceHeatmapRequest(HttpServletRequest request);
}
