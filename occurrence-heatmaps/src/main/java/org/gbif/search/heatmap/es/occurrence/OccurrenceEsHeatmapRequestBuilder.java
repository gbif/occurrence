package org.gbif.search.heatmap.es.occurrence;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.occurrence.search.es.RequestFieldsTranslator;
import org.gbif.predicate.query.EsFieldMapper;
import org.gbif.predicate.query.OccurrenceEsQueryVisitor;
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.gbif.search.heatmap.es.BaseEsHeatmapRequestBuilder;
import org.gbif.search.heatmap.occurrence.OccurrenceHeatmapRequest;
import org.gbif.vocabulary.client.ConceptClient;

public class OccurrenceEsHeatmapRequestBuilder
    extends BaseEsHeatmapRequestBuilder<OccurrenceSearchParameter, OccurrenceHeatmapRequest> {

  public OccurrenceEsHeatmapRequestBuilder(
      EsFieldMapper<OccurrenceSearchParameter> esFieldMapper,
      ConceptClient conceptClient,
      NameUsageMatchingService nameUsageMatchingService) {
    super(
        esFieldMapper,
        conceptClient,
        nameUsageMatchingService,
        new OccurrenceEsQueryVisitor(esFieldMapper));
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
