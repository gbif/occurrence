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
package org.gbif.search.heatmap.es.occurrence;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.occurrence.search.es.RequestFieldsTranslator;
import org.gbif.predicate.query.EsFieldMapper;
import org.gbif.predicate.query.OccurrenceEsQueryVisitor;
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.gbif.search.heatmap.es.BaseEsHeatmapRequestBuilder;
import org.gbif.search.heatmap.occurrence.OccurrenceHeatmapRequest;
import org.gbif.vocabulary.client.ConceptClient;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.elasticsearch.index.query.BoolQueryBuilder;

public class OccurrenceEsHeatmapRequestBuilder
    extends BaseEsHeatmapRequestBuilder<OccurrenceSearchParameter, OccurrenceHeatmapRequest> {

  public OccurrenceEsHeatmapRequestBuilder(
      EsFieldMapper<OccurrenceSearchParameter> esFieldMapper,
      ConceptClient conceptClient,
      NameUsageMatchingService nameUsageMatchingService,
      String defaultChecklistKey,
      int defaultShardSize) {
    super(
        esFieldMapper,
        conceptClient,
        nameUsageMatchingService,
        new OccurrenceEsQueryVisitor(esFieldMapper, defaultChecklistKey),
        defaultChecklistKey,
        defaultShardSize);
  }

  @Override
  protected void handleIssueQueries(Map<OccurrenceSearchParameter, Set<String>> params, BoolQueryBuilder bool) {
    super.handleOccurrenceIssueQueries(params, bool);
  }

  @Override
  protected Optional<OccurrenceSearchParameter> getParam(String name) {
    return OccurrenceSearchParameter.lookup(name);
  }

  @Override
  protected OccurrenceSearchParameter createSearchParam(String name, Class<?> type) {
    return new OccurrenceSearchParameter(name, type);
  }

  @Override
  protected void translateFields(Map<OccurrenceSearchParameter, Set<String>> params) {
    RequestFieldsTranslator.translateOccurrenceFields(params, conceptClient);
  }

  @Override
  protected Predicate translatePredicateFields(Predicate predicate) {
    return RequestFieldsTranslator.translateOccurrencePredicateFields(predicate, conceptClient);
  }
}
