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
package org.gbif.search.heatmap.occurrence;

import jakarta.servlet.http.HttpServletRequest;
import java.util.Optional;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.occurrence.search.cache.PredicateCacheService;
import org.gbif.search.heatmap.BaseHeatmapRequestProvider;
import org.gbif.ws.util.ParamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OccurrenceHeatmapRequestProvider
    extends BaseHeatmapRequestProvider<OccurrenceSearchParameter, OccurrenceHeatmapRequest> {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceHeatmapRequestProvider.class);

  /**
   * Making constructor private.
   *
   * @param predicateCacheService
   */
  public OccurrenceHeatmapRequestProvider(PredicateCacheService predicateCacheService) {
    super(predicateCacheService);
  }

  @Override
  protected OccurrenceHeatmapRequest createEmptyRequest() {
    return new OccurrenceHeatmapRequest();
  }

  @Override
  protected Optional<OccurrenceSearchParameter> findSearchParam(String name) {
    return OccurrenceSearchParameter.lookup(name);
  }

  @Override
  protected void setSearchParams(
      OccurrenceHeatmapRequest heatmapSearchRequest, HttpServletRequest request) {
    super.setSearchParams(heatmapSearchRequest, request);
    ParamUtils.convertDnaSequenceParam(request.getParameterMap(), heatmapSearchRequest);
  }
}
