package org.gbif.occurrence.ws.client;

import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.ws.client.BaseWsFacetedSearchClient;

import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.inject.Inject;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;

import static org.gbif.api.model.common.paging.PagingConstants.PARAM_LIMIT;
import static org.gbif.api.model.common.search.SearchConstants.DEFAULT_SUGGEST_LIMIT;
import static org.gbif.api.model.common.search.SearchConstants.QUERY_PARAM;
import static org.gbif.ws.paths.OccurrencePaths.CATALOG_NUMBER_PATH;
import static org.gbif.ws.paths.OccurrencePaths.COLLECTION_CODE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.INSTITUTION_CODE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.OCCURRENCE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.RECORDED_BY_PATH;
import static org.gbif.ws.paths.OccurrencePaths.RECORD_NUMBER_PATH;
import static org.gbif.ws.paths.OccurrencePaths.OCCURRENCE_ID_PATH;

/**
 * Ws client for {@link OccurrenceSearchService}.
 */
public class OccurrenceWsSearchClient extends BaseWsFacetedSearchClient<Occurrence, OccurrenceSearchParameter, OccurrenceSearchRequest>
  implements OccurrenceSearchService {

  private static final String SEARCH_PATH ="search/";

  // Response type.
  private static final GenericType<SearchResponse<Occurrence, OccurrenceSearchParameter>> GENERIC_TYPE =
    new GenericType<SearchResponse<Occurrence, OccurrenceSearchParameter>>() {
    };

  // List<String> type
  private static final GenericType<List<String>> LIST_OF_STRINGS_TYPE =
    new GenericType<List<String>>() {
    };

  /**
   * @param resource to the occurrence webapp
   */
  @Inject
  protected OccurrenceWsSearchClient(WebResource resource) {
    super(resource.path(OCCURRENCE_PATH), GENERIC_TYPE);
  }

  @Override
  public List<String> suggestCatalogNumbers(String prefix, @Nullable Integer limit) {
    return suggestTerms(CATALOG_NUMBER_PATH, prefix, limit);
  }

  @Override
  public List<String> suggestCollectionCodes(String prefix, @Nullable Integer limit) {
    return suggestTerms(COLLECTION_CODE_PATH, prefix, limit);
  }

  @Override
  public List<String> suggestRecordedBy(String prefix, @Nullable Integer limit) {
    return suggestTerms(RECORDED_BY_PATH, prefix, limit);
  }

  @Override
  public List<String> suggestRecordNumbers(String prefix, @Nullable Integer limit) {
    return suggestTerms(RECORD_NUMBER_PATH, prefix, limit);
  }

  @Override
  public List<String> suggestInstitutionCodes(String prefix, @Nullable Integer limit) {
    return suggestTerms(INSTITUTION_CODE_PATH, prefix, limit);
  }

  @Override
  public List<String> suggestOccurrenceIds(String prefix, @Nullable Integer limit) {
    return suggestTerms(OCCURRENCE_ID_PATH, prefix, limit);
  }

  /**
   * Utility function that execute a search term query.
   */
  private List<String> suggestTerms(String resourceName, String prefix, @Nullable Integer limit) {
    String limitParam = Integer.toString(MoreObjects.firstNonNull(limit, DEFAULT_SUGGEST_LIMIT));
    return getResource(SEARCH_PATH + resourceName).queryParam(QUERY_PARAM, prefix).queryParam(PARAM_LIMIT, limitParam)
      .get(LIST_OF_STRINGS_TYPE);
  }
}
