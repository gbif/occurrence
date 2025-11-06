package org.gbif.search.heatmap.es.event;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.gbif.api.model.event.search.EventSearchParameter;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.occurrence.search.es.RequestFieldsTranslator;
import org.gbif.predicate.query.EsFieldMapper;
import org.gbif.predicate.query.EventEsQueryVisitor;
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.gbif.search.heatmap.es.BaseEsHeatmapRequestBuilder;
import org.gbif.search.heatmap.event.EventHeatmapRequest;
import org.gbif.vocabulary.client.ConceptClient;

public class EventEsHeatmapRequestBuilder
    extends BaseEsHeatmapRequestBuilder<EventSearchParameter, EventHeatmapRequest> {

  public EventEsHeatmapRequestBuilder(
      EsFieldMapper<EventSearchParameter> esFieldMapper,
      ConceptClient conceptClient,
      NameUsageMatchingService nameUsageMatchingService) {
    super(
        esFieldMapper,
        conceptClient,
        nameUsageMatchingService,
        new EventEsQueryVisitor(esFieldMapper));
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
