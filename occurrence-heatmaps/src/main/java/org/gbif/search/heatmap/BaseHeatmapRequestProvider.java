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
package org.gbif.search.heatmap;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.gbif.api.model.common.search.PredicateSearchRequest;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.common.search.SearchRequest;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.util.SearchTypeValidator;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.occurrence.search.cache.PredicateCacheService;
import org.gbif.search.heatmap.occurrence.OccurrenceHeatmapRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseHeatmapRequestProvider<
        P extends SearchParameter,
        R extends HeatmapRequest & PredicateSearchRequest & SearchRequest<P>>
    implements HeatmapRequestProvider<R> {

  public static final String POLYGON_PATTERN = "POLYGON((%s))";
  public static final String PARAM_QUERY_STRING = "q";
  public static final String GEOM_PARAM = "geom";
  public static final String ZOOM_PARAM = "z";
  public static final String MODE_PARAM = "mode";
  public static final String BUCKET_LIMIT_PARAM = "bucketLimit";
  public static final String PARAM_PREDICATE_HASH = "predicateHash";
  private static final int DEFAULT_ZOOM_LEVEL = 3;
  public static final int DEFAULT_BUCKET_LIMIT = 15000;
  private static final Logger LOG = LoggerFactory.getLogger(BaseHeatmapRequestProvider.class);

  private final PredicateCacheService predicateCacheService;

  /** Making constructor private. */
  public BaseHeatmapRequestProvider(PredicateCacheService predicateCacheService) {
    this.predicateCacheService = predicateCacheService;
  }

  /** Translate the raw value into value that API understands. */
  private String translateFilterValue(P param, String value) {
    Optional<P> geometryParam = findSearchParam(OccurrenceSearchParameter.GEOMETRY.name());
    if (geometryParam.isPresent() && param == geometryParam.get()) {
      try { // checks if the parameters is in WKT format
        SearchTypeValidator.validate(geometryParam.get(), value);
        return value;
      } catch (IllegalArgumentException ex) { // if not is WKT, assumes to be a POLYGON
        return String.format(POLYGON_PATTERN, value);
      }
    }
    if (Enum.class.isAssignableFrom(param.type())) {
      return value.toUpperCase();
    }
    return value;
  }

  @Override
  public R buildOccurrenceHeatmapRequest(HttpServletRequest request) {
    R heatmapSearchRequest = createEmptyRequest();

    String q = request.getParameter(PARAM_QUERY_STRING);

    if (!Strings.isNullOrEmpty(q)) {
      heatmapSearchRequest.setQ(q);
    }
    // find search parameter enum based filters
    setSearchParams(heatmapSearchRequest, request);

    String predicateHash = request.getParameter(PARAM_PREDICATE_HASH);
    if (!Strings.isNullOrEmpty(predicateHash)) {
      heatmapSearchRequest.setPredicate(
          Optional.ofNullable(predicateCacheService.get(Integer.parseInt(predicateHash)))
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          PARAM_PREDICATE_HASH + " " + predicateHash + " not found")));
    }

    return heatmapSearchRequest;
  }

  protected abstract R createEmptyRequest();

  /**
   * Iterates over the params map and adds to the search request the recognized parameters (i.e.:
   * those that have a correspondent value in the P generic parameter). Empty (of all size) and null
   * parameters are discarded.
   */
  protected void setSearchParams(R heatmapSearchRequest, HttpServletRequest request) {
    for (Map.Entry<String, String[]> entry : request.getParameterMap().entrySet()) {
      findSearchParam(entry.getKey())
          .ifPresent(
              p -> {
                for (String val : removeEmptyParameters(entry.getValue())) {
                  String translatedVal =
                      translateFilterValue(p, val); // this transformation is require
                  SearchTypeValidator.validate(p, translatedVal);
                  heatmapSearchRequest.addParameter(p, translatedVal);
                }
              });
    }

    heatmapSearchRequest.setZoom(getIntParam(request, ZOOM_PARAM, DEFAULT_ZOOM_LEVEL));
    if (request.getParameterMap().containsKey(GEOM_PARAM)) {
      heatmapSearchRequest.setGeometry(request.getParameterMap().get(GEOM_PARAM)[0]);
    }
    heatmapSearchRequest.setMode(getMode(request));
    heatmapSearchRequest.setBucketLimit(
        getIntParam(request, BUCKET_LIMIT_PARAM, DEFAULT_BUCKET_LIMIT));

    LOG.debug("Querying using Geometry {}", heatmapSearchRequest.getGeometry());
  }

  /**
   * Gets the Map mode, if the MODE_PARAM is not presents, returns the default
   * OccurrenceHeatmapRequest.Mode.GEO_BOUNDS.
   */
  private static OccurrenceHeatmapRequest.Mode getMode(HttpServletRequest request) {
    return Optional.ofNullable(request.getParameter(MODE_PARAM))
        .map(mode -> VocabularyUtils.lookupEnum(mode, OccurrenceHeatmapRequest.Mode.class))
        .orElse(OccurrenceHeatmapRequest.Mode.GEO_BOUNDS);
  }

  protected abstract Optional<P> findSearchParam(String name);

  /**
   * Removes all empty and null parameters from the list. Each value is trimmed(String.trim()) in
   * order to remove all sizes of empty parameters.
   */
  private static List<String> removeEmptyParameters(String[] parameters) {
    List<String> cleanParameters = Lists.newArrayListWithCapacity(parameters.length);
    for (String param : parameters) {
      String cleanParam = Strings.nullToEmpty(param).trim();
      if (!cleanParam.isEmpty()) {
        cleanParameters.add(cleanParam);
      }
    }
    return cleanParameters;
  }

  private static int getIntParam(HttpServletRequest request, String param, int defaultVal) {
    String[] vals = request.getParameterValues(param);
    if (vals != null && vals.length > 0) {
      try {
        return Integer.parseInt(vals[0]);
      } catch (NumberFormatException e) {
        return defaultVal;
      }
    }
    return defaultVal;
  }
}
