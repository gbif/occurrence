package org.gbif.event.search.es;

import org.gbif.api.model.event.search.EventSearchParameter;
import org.gbif.api.model.event.search.EventSearchRequest;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.occurrence.search.es.BaseEsSearchRequestBuilder;
import org.gbif.occurrence.search.es.RequestFieldsTranslator;
import org.gbif.predicate.query.EventEsQueryVisitor;
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.gbif.search.es.event.EventEsFieldMapper;
import org.gbif.vocabulary.client.ConceptClient;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;

public class EventEsSearchRequestBuilder
    extends BaseEsSearchRequestBuilder<EventSearchParameter, EventSearchRequest> {

  public EventEsSearchRequestBuilder(
      EventEsFieldMapper eventEsFieldMapper,
      ConceptClient conceptClient,
      NameUsageMatchingService nameUsageMatchingService,
      String defaultChecklistKey,
      int defaultShardSize) {
    super(
        eventEsFieldMapper,
        conceptClient,
        nameUsageMatchingService,
        new EventEsQueryVisitor(eventEsFieldMapper, defaultChecklistKey),
        defaultChecklistKey,
        defaultShardSize);
  }

  @Override
  protected void handleIssueQueries(Map<EventSearchParameter, Set<String>> params, List<Query> filters) {
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
