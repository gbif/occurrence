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
package org.gbif.search.heatmap.event;

import java.util.Optional;
import org.gbif.api.model.event.search.EventSearchParameter;
import org.gbif.occurrence.search.cache.PredicateCacheService;
import org.gbif.search.heatmap.BaseHeatmapRequestProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventHeatmapRequestProvider
    extends BaseHeatmapRequestProvider<EventSearchParameter, EventHeatmapRequest> {

  private static final Logger LOG = LoggerFactory.getLogger(EventHeatmapRequestProvider.class);

  /**
   * Making constructor private.
   *
   * @param predicateCacheService
   */
  public EventHeatmapRequestProvider(PredicateCacheService predicateCacheService) {
    super(predicateCacheService);
  }

  @Override
  protected EventHeatmapRequest createEmptyRequest() {
    return new EventHeatmapRequest();
  }

  @Override
  protected Optional<EventSearchParameter> findSearchParam(String name) {
    return EventSearchParameter.lookupEventParam(name);
  }
}
