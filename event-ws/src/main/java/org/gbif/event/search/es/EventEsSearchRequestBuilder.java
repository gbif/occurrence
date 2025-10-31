package org.gbif.event.search.es;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.gbif.api.model.event.search.EventSearchParameter;
import org.gbif.api.model.event.search.EventSearchRequest;
import org.gbif.occurrence.search.es.BaseEsSearchRequestBuilder;
import org.gbif.occurrence.search.es.RequestFieldsTranslator;
import org.gbif.predicate.query.EventEsQueryVisitor;
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.gbif.search.es.EsField;
import org.gbif.search.es.event.EventEsFieldMapper;
import org.gbif.vocabulary.client.ConceptClient;

public class EventEsSearchRequestBuilder
    extends BaseEsSearchRequestBuilder<EventSearchParameter, EventSearchRequest> {

  private final EventEsFieldMapper eventEsFieldMapper;

  public EventEsSearchRequestBuilder(
      EventEsFieldMapper eventEsFieldMapper,
      ConceptClient conceptClient,
      NameUsageMatchingService nameUsageMatchingService,
      EventEsQueryVisitor esQueryVisitor) {
    super(eventEsFieldMapper, conceptClient, nameUsageMatchingService, esQueryVisitor);
    this.eventEsFieldMapper = eventEsFieldMapper;
  }

  @Override
  protected EsField getEsField(EventSearchParameter parameter) {
    return eventEsFieldMapper.getEsField(parameter);
  }

  @Override
  protected EsField getEsFacetField(EventSearchParameter parameter) {
    return eventEsFieldMapper.getEsFacetField(parameter);
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
  protected boolean isDateField(EsField esField) {
    return eventEsFieldMapper.isDateField(esField);
  }

  @Override
  protected EsField getGeoDistanceEsField() {
    return eventEsFieldMapper.getGeoDistanceEsField();
  }

  @Override
  protected EsField getGeoShapeEsField() {
    return eventEsFieldMapper.getGeoShapeEsField();
  }

  @Override
  protected void translateVocabs(Map<EventSearchParameter, Set<String>> params) {
    RequestFieldsTranslator.translateEventFields(params, conceptClient);
  }
}
