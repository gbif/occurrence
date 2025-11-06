package org.gbif.event.search.es;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.gbif.api.model.event.search.EventSearchParameter;
import org.gbif.api.model.event.search.EventSearchRequest;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.occurrence.search.es.BaseEsSearchRequestBuilder;
import org.gbif.occurrence.search.es.RequestFieldsTranslator;
import org.gbif.predicate.query.EventEsQueryVisitor;
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.gbif.search.es.event.EventEsFieldMapper;
import org.gbif.vocabulary.client.ConceptClient;

public class EventEsSearchRequestBuilder
    extends BaseEsSearchRequestBuilder<EventSearchParameter, EventSearchRequest> {

  public EventEsSearchRequestBuilder(
      EventEsFieldMapper eventEsFieldMapper,
      ConceptClient conceptClient,
      NameUsageMatchingService nameUsageMatchingService) {
    super(
        eventEsFieldMapper,
        conceptClient,
        nameUsageMatchingService,
        new EventEsQueryVisitor(eventEsFieldMapper));
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
