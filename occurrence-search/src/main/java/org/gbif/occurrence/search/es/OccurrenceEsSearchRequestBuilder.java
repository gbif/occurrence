package org.gbif.occurrence.search.es;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.predicate.query.OccurrenceEsQueryVisitor;
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.gbif.search.es.EsField;
import org.gbif.search.es.occurrence.OccurrenceEsFieldMapper;
import org.gbif.vocabulary.client.ConceptClient;

public class OccurrenceEsSearchRequestBuilder
    extends BaseEsSearchRequestBuilder<OccurrenceSearchParameter, OccurrenceSearchRequest> {

  private final OccurrenceEsFieldMapper occurrenceEsFieldMapper;

  public OccurrenceEsSearchRequestBuilder(
    OccurrenceEsFieldMapper occurrenceEsFieldMapper,
    ConceptClient conceptClient,
    NameUsageMatchingService nameUsageMatchingService,
    OccurrenceEsQueryVisitor occurrenceEsQueryVisitor) {
    super(
        occurrenceEsFieldMapper, conceptClient, nameUsageMatchingService, occurrenceEsQueryVisitor);
    this.occurrenceEsFieldMapper = occurrenceEsFieldMapper;
  }

  @Override
  protected EsField getEsField(OccurrenceSearchParameter parameter) {
    return occurrenceEsFieldMapper.getEsField(parameter);
  }

  @Override
  protected EsField getEsFacetField(OccurrenceSearchParameter parameter) {
    return occurrenceEsFieldMapper.getEsFacetField(parameter);
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
  protected boolean isDateField(EsField esField) {
    return occurrenceEsFieldMapper.isDateField(esField);
  }

  @Override
  protected EsField getGeoDistanceEsField() {
    return occurrenceEsFieldMapper.getGeoDistanceEsField();
  }

  @Override
  protected EsField getGeoShapeEsField() {
    return occurrenceEsFieldMapper.getGeoShapeEsField();
  }

  @Override
  protected void translateVocabs(Map<OccurrenceSearchParameter, Set<String>> params) {
    RequestFieldsTranslator.translateOccurrenceFields(params, conceptClient);
  }
}
