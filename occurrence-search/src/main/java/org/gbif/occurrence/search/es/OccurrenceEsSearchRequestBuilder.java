package org.gbif.occurrence.search.es;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.predicate.query.OccurrenceEsQueryVisitor;
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.gbif.search.es.occurrence.OccurrenceEsFieldMapper;
import org.gbif.vocabulary.client.ConceptClient;

public class OccurrenceEsSearchRequestBuilder
    extends BaseEsSearchRequestBuilder<OccurrenceSearchParameter, OccurrenceSearchRequest> {

  public OccurrenceEsSearchRequestBuilder(
      OccurrenceEsFieldMapper occurrenceEsFieldMapper,
      ConceptClient conceptClient,
      NameUsageMatchingService nameUsageMatchingService) {
    super(
        occurrenceEsFieldMapper,
        conceptClient,
        nameUsageMatchingService,
        new OccurrenceEsQueryVisitor(occurrenceEsFieldMapper));
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
