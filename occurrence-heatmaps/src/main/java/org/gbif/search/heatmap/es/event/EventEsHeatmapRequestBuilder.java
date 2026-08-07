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

import org.gbif.api.model.event.search.EventSearchParameter;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.occurrence.search.es.RequestFieldsTranslator;
import org.gbif.predicate.query.EsFieldMapper;
import org.gbif.predicate.query.EventEsQueryVisitor;
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.gbif.search.heatmap.es.BaseEsHeatmapRequestBuilder;
import org.gbif.search.heatmap.event.EventHeatmapRequest;
import org.gbif.vocabulary.client.ConceptClient;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.elasticsearch.index.query.BoolQueryBuilder;

public class EventEsHeatmapRequestBuilder
    extends BaseEsHeatmapRequestBuilder<EventSearchParameter, EventHeatmapRequest> {

  public EventEsHeatmapRequestBuilder(
      EsFieldMapper<EventSearchParameter> esFieldMapper,
      ConceptClient conceptClient,
      NameUsageMatchingService nameUsageMatchingService,
      String defaultChecklistKey,
      int defaultShardSize) {
    super(
        esFieldMapper,
        conceptClient,
        nameUsageMatchingService,
        new EventEsQueryVisitor(esFieldMapper, defaultChecklistKey),
        defaultChecklistKey,
        defaultShardSize);
  }

  @Override
  protected void handleIssueQueries(Map<EventSearchParameter, Set<String>> params, BoolQueryBuilder bool) {
    // do nothing
  }

  @Override
  protected Optional<EventSearchParameter> getParam(String name) {
    return EventSearchParameter.lookupEventParam(name);
  }

  @Override
  protected EventSearchParameter createSearchParam(String name, Class<?> type) {
    return new EventSearchParameter(name, type);
  }

  @Override
  protected void translateFields(Map<EventSearchParameter, Set<String>> params) {
    RequestFieldsTranslator.translateEventFields(params, conceptClient);
  }

  @Override
  protected Predicate translatePredicateFields(Predicate predicate) {
    return RequestFieldsTranslator.translateEventPredicateFields(predicate, conceptClient);
  }
}
