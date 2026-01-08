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
package org.gbif.search.heatmap.es.event;

import org.elasticsearch.client.RestHighLevelClient;
import org.gbif.api.model.event.search.EventSearchParameter;
import org.gbif.search.heatmap.es.BaseHeatmapsEsService;
import org.gbif.search.heatmap.event.EventHeatmapRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Elasticsearch heatmap service. */
@Component
public class EventHeatmapsEsService
    extends BaseHeatmapsEsService<EventSearchParameter, EventHeatmapRequest> {

  private static final Logger LOG = LoggerFactory.getLogger(EventHeatmapsEsService.class);

  public EventHeatmapsEsService(
      RestHighLevelClient esClient,
      String esIndex,
      EventEsHeatmapRequestBuilder esHeatmapRequestBuilder) {
    super(esClient, esIndex, esHeatmapRequestBuilder);
  }
}
