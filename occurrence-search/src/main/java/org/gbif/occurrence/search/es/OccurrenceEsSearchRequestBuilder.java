package org.gbif.occurrence.search.es;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.elasticsearch.index.query.BoolQueryBuilder;
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
      NameUsageMatchingService nameUsageMatchingService,
      String defaultChecklistKey) {
    super(
        occurrenceEsFieldMapper,
        conceptClient,
        nameUsageMatchingService,
        new OccurrenceEsQueryVisitor(occurrenceEsFieldMapper, defaultChecklistKey),
        defaultChecklistKey);
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

  /**
   * If a user specifies a checklistKey and an Issue query, then we use the new NON_TAXONOMIC_ISSUE
   * which doesnt contain taxonomic issues, which are now stored in a separate array, one per
   * checklist.
   *
   * @param params the search parameters
   * @param bool the bool query builder
   */
  @Override
  protected void handleIssueQueries(
      Map<OccurrenceSearchParameter, Set<String>> params, BoolQueryBuilder bool) {
    super.handleOccurrenceIssueQueries(params, bool);
  }
}
